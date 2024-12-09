from datadog import statsd
from functools import lru_cache
import glob
import json
from os import path
import re
from time import time
import pyarrow.parquet as pq
from sqlalchemy import MetaData, Table, create_engine, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from .logging import LOGGER
from .s3 import parse_parquet_filename
from .settings import Settings

# TODO: detect this from the table
# arrays and json columns are stored as json in parquet because that was easier than dealing with the schema
JSON_COLUMNS = [
    "embeds",
    "mentions",
    "mentions_positions",
    "verified_addresses",
]


def init_db(uri, parquet_tables, settings: Settings):
    """Initialize the database with our simple schema."""
    engine = create_engine(uri, pool_size=settings.postgres_pool_size, max_overflow=2)

    LOGGER.info("migrating...")
    with engine.connect() as connection:
        # set the schema if we have one configured. otherwise everything goes into "public"
        if settings.postgres_schema:
            connection.execute(
                "SET search_path TO :schema_name",
                {"schema_name": settings.postgres_schema},
            )

        pattern = r"schema/(?P<num>\d{3})_(?P<parquet_db_name>[a-zA-Z0-9-]+)_(?P<parquet_schema_name>[a-zA-Z0-9-]+)_(?P<parquet_table_name>[a-zA-Z0-9_]+)\.sql"

        for filename in sorted(glob.glob("schema/*.sql")):
            m = re.match(pattern, filename)

            if m:
                parts = m.groupdict()

                LOGGER.debug(parts)

                if parts["parquet_db_name"] == "all":
                    pass
                elif (
                    parts["parquet_db_name"] == settings.parquet_s3_database
                    and parts["parquet_schema_name"] == settings.parquet_s3_schema
                    and parts["parquet_table_name"] in parquet_tables
                ):
                    pass
                else:
                    LOGGER.info("Skipping %s", filename)
                    continue
            else:
                LOGGER.info("Skipping %s", filename)
                continue

            LOGGER.info("Applying %s", filename)

            with open(filename, "r") as f:
                connection.execute(text(f.read()))

        # TODO: honestly not sure why i need this. i thought it would auto commit
        connection.commit()

    LOGGER.info("migrations complete.")

    return engine


def check_for_existing_incremental_import(engine, settings: Settings, table_name):
    """Returns the filename for the newest incremental. This may only be partially imported."""

    parquet_import_tracking = get_table(engine, "parquet_import_tracking")

    # TODO: make this much smarter. it needs to find the last one that was fully imported. there might be holes if we run things in parallel!
    stmt = (
        select(
            parquet_import_tracking.c.file_name,
        )
        .where(parquet_import_tracking.c.file_type == "incremental")
        .where(parquet_import_tracking.c.table_name == table_name)
        .where(parquet_import_tracking.c.file_version == settings.npe_version)
        .where(
            parquet_import_tracking.c.file_duration_s == settings.incremental_duration
        )
        .order_by(parquet_import_tracking.c.imported_at.desc())
        .limit(1)
    )

    with engine.connect() as conn:
        result = conn.execute(stmt).fetchone()

    if result is None:
        return None

    return result[0]


def check_for_existing_full_import(engine, settings: Settings, table_name):
    """Returns the filename of the newest full import (there should really only be one). This may only be partially imported."""

    parquet_import_tracking = get_table(engine, "parquet_import_tracking")

    stmt = (
        select(
            parquet_import_tracking.c.file_name,
        )
        .where(parquet_import_tracking.c.file_type == "full")
        .where(parquet_import_tracking.c.table_name == table_name)
        .where(parquet_import_tracking.c.file_version == settings.npe_version)
        .where(
            parquet_import_tracking.c.file_duration_s == settings.incremental_duration
        )
        .order_by(parquet_import_tracking.c.imported_at.desc())
        .limit(1)
    )

    with engine.connect() as conn:
        result = conn.execute(stmt).fetchone()

    if result is None:
        return None

    return result[0]


def clean_parquet_data(col_name, value):
    if col_name in JSON_COLUMNS:
        return json.loads(value)
    return value


@lru_cache(None)
def get_table(engine, table_name):
    metadata = MetaData()
    return Table(table_name, metadata, autoload_with=engine)


def import_parquet(
    engine,
    table_name,
    local_filename,
    file_type,
    progress_callback,
    empty_callback,
    settings: Settings,
):
    parsed_filename = parse_parquet_filename(local_filename)

    assert table_name == parsed_filename["table_name"]
    schema_name = parsed_filename["schema_name"]

    dd_tags = [
        f"parquet_table:{schema_name}.{table_name}",
    ]

    table = get_table(engine, table_name)
    tracking_table = get_table(engine, "parquet_import_tracking")

    is_empty = local_filename.endswith(".empty")

    if is_empty:
        num_row_groups = 0
        # TODO: maybe have an option to return here instead of saving the ".empty" into the database
    else:
        parquet_file = pq.ParquetFile(local_filename)
        num_row_groups = parquet_file.num_row_groups

    with engine.connect() as conn:
        # Do NOT put 0 here. That would mean that we already imported row group 0!
        last_row_group_imported = None

        # Prepare the insert statement
        stmt = pg_insert(tracking_table).values(
            table_name=table_name,
            file_name=local_filename,
            file_type=file_type,
            file_version=settings.npe_version,
            file_duration_s=settings.incremental_duration,
            is_empty=is_empty,
            last_row_group_imported=last_row_group_imported,
            total_row_groups=num_row_groups,
        )

        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=["file_name"],  # Use the unique constraint columns
            set_={
                # No actual data changes; this is a no-op update
                "last_row_group_imported": tracking_table.c.last_row_group_imported,
            },
        ).returning(tracking_table.c.id, tracking_table.c.last_row_group_imported)

        # Execute the statement and fetch the result
        result = conn.execute(upsert_stmt)
        row = result.fetchone()

        # Extract the id and last_row_group_imported
        tracking_id = row.id
        last_row_group_imported = row.last_row_group_imported

        if is_empty:
            LOGGER.debug(
                "Imported empty file with id %s: %s", tracking_id, local_filename
            )
            # no need to continue on here. we can return early
            empty_callback(1)
            conn.commit()

            age_s = time() - parsed_filename["end_timestamp"]

            dogstatsd.gauge("parquet_rows_age_s", age_s, tags=dd_tags)

            return

        if last_row_group_imported is None:
            start_row_group = 0
        else:
            start_row_group = last_row_group_imported + 1

        new_steps = num_row_groups - start_row_group

        if new_steps == 0:
            LOGGER.info("%s has already been imported", local_filename)
            return

        if last_row_group_imported is not None:
            LOGGER.info("%s has resumed importing", local_filename)

        # LOGGER.debug(
        #     "%s more steps from %s",
        #     f"{new_steps:_}",
        #     table_name,
        # )

        # update the progress counter with our new step total
        progress_callback.more_steps(new_steps)

        primary_key_columns = table.primary_key.columns.values()

        # Read the data in batches
        # TODO: parallelize the clean_parquet_data. the inserts still need to happen in order so that tracking doesn't get mixed up
        for i in range(start_row_group, num_row_groups):
            LOGGER.debug(
                "Upsert #%s/%s for %s", f"{i+1:_}", f"{num_row_groups:_}", table_name
            )

            # TODO: larger batches with `pf.iter_batches(batch_size=X)` instead of row groups
            batch = parquet_file.read_row_group(i)

            data = batch.to_pydict()

            # collect into a different dict so that we can remove dupes
            # NOTE: You can modify the data however you want here. Do things like pull values out of json columns or skip columns entirely.
            # TODO: have a helper function here that makes it easier to clean up the data
            rows = {
                tuple(
                    clean_parquet_data(pk_col.name, data[pk_col.name][i])
                    for pk_col in primary_key_columns
                ): {
                    col_name: clean_parquet_data(col_name, data[col_name][i])
                    for col_name in data
                }
                for i in range(len(batch))
            }

            # discard the keys
            rows = list(rows.values())

            if len(batch) > len(rows):
                LOGGER.debug(
                    "Dropping %s rows with duplicate primary keys",
                    len(batch) - len(rows),
                )

            # insert or update the rows
            stmt = pg_insert(table).values(rows)

            # TODO: only upsert where updated_at is newer than the existing row
            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=primary_key_columns,
                set_={col: stmt.excluded[col] for col in data.keys()},
                where=(stmt.excluded["updated_at"] > table.c.updated_at),
            )

            conn.execute(upsert_stmt)

            # update our database entry's last_row_group_imported
            update_tracking_stmt = (
                tracking_table.update()
                .where(tracking_table.c.id == tracking_id)
                .values(last_row_group_imported=i)
            )
            conn.execute(update_tracking_stmt)

            # save the rows and the tracking update together
            conn.commit()

            progress_callback(1)

            age_s = time() - parsed_filename["end_timestamp"]

            statsd.gauge("parquet_rows_import_age_s", age_s, tags=dd_tags)
            statsd.increment(
                "num_parquet_rows_imported",
                value=len(batch),
                tags=dd_tags,
            )

        file_size = path.getsize(local_filename)

        statsd.increment(
            "parquet_bytes_imported",
            value=file_size,
            tags=dd_tags,
        )

        # TODO: datadog metrics instead?
        LOGGER.info(
            "finished import",
            extra={
                "age_s": age_s,
                "table_name": table_name,
                "file_name": local_filename,
                "num_row_groups": num_row_groups,
                "num_rows": parquet_file.metadata.num_rows,
                "file_size": file_size,
            },
        )
