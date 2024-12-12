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

from .logger import LOGGER
from .s3 import parse_parquet_filename
from .settings import Settings, SHUTDOWN_EVENT

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
    engine = create_engine(
        uri,
        pool_size=settings.postgres_pool_size,
        isolation_level="AUTOCOMMIT",
        pool_reset_on_return=None,
        pool_timeout=120,
    )

    LOGGER.info("migrating...")
    with engine.connect() as conn:
        # set the schema if we have one configured. otherwise everything goes into "public"
        if settings.postgres_schema:
            conn.execute(
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
                    LOGGER.debug("Skipping %s", filename)
                    continue
            else:
                LOGGER.debug("Skipping %s", filename)
                continue

            LOGGER.info("Applying %s", filename)

            with open(filename, "r") as f:
                conn.execute(text(f.read()))

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
    # TODO: if this is a datetime column, it is from parquet in milliseconds, not seconds!
    return value


@lru_cache(maxsize=1)
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
    row_group_executor,
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

    with engine.connect() as conn:
        # Execute the statement and fetch the result
        result = conn.execute(upsert_stmt)
        row = result.fetchone()

    # Extract the id and last_row_group_imported
    tracking_id = row.id
    last_row_group_imported = row.last_row_group_imported

    if is_empty:
        # LOGGER.debug(
        #     "imported empty file", extra={"id": tracking_id, "file": local_filename}
        # )

        # no need to continue on here. we can commit and return early
        empty_callback(1)

        file_age_s = time() - parsed_filename["end_timestamp"]

        statsd.gauge("parquet_file_age_s", file_age_s, tags=dd_tags)

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
    # TODO: parallelize this. the import tracking needs to be done in order though!
    fs = []
    for i in range(start_row_group, num_row_groups):
        # TODO: larger batches with `pf.iter_batches(batch_size=X)` instead of row groups

        # LOGGER.debug(
        #     "Queueing upsert #%s/%s for %s",
        #     f"{i+1:_}",
        #     f"{num_row_groups:_}",
        #     table_name,
        # )

        f = row_group_executor.submit(
            process_batch,
            parquet_file,
            i,
            engine,
            primary_key_columns,
            table,
            progress_callback,
            parsed_filename,
            dd_tags,
        )

        fs.append(f)

        # TODO: if fs is really long, wait? or maybe do update_tracking_stmt here, too?

    # LOGGER.debug("waiting for %s futures", len(fs))

    # read them in order rather than with as_completed
    # TODO: this saves progress slower than i expected
    # TODO: drain this so that we don't hold a ton of memory
    file_age_s = row_age_s = None
    while fs:
        if SHUTDOWN_EVENT.is_set():
            row_group_executor.shutdown(cancel_futures=True, wait=False)
            break

        try:
            (i, file_age_s, row_age_s) = fs[0].result(timeout=2)
        except TimeoutError:
            # LOGGER.debug("TimeoutError")
            continue

        fs.pop(0)

        # update our database entry's last_row_group_imported
        # TODO: move this outside this function so that we can do them in order while doing this function in parallel
        update_tracking_stmt = (
            tracking_table.update()
            .where(tracking_table.c.id == tracking_id)
            .values(last_row_group_imported=i)
        )

        # TODO: connect inside or outside the loop?
        with engine.connect() as conn:
            conn.execute(update_tracking_stmt)

        # TODO: metric here?
        LOGGER.debug(
            "Completed upsert #%s/%s for %s",
            f"{i+1:_}",
            f"{num_row_groups:_}",
            table_name,
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
            "file_age_s": file_age_s,
            "row_age_s": row_age_s,
            "table_name": table_name,
            "file_name": local_filename,
            "num_row_groups": num_row_groups,
            "num_rows": parquet_file.metadata.num_rows,
            "file_size": file_size,
        },
    )


def process_batch(
    parquet_file,
    i,
    engine,
    primary_key_columns,
    table,
    progress_callback,
    parsed_filename,
    dd_tags,
):
    # LOGGER.debug("starting batch #%s", i)

    batch = parquet_file.read_row_group(i)

    # TODO: use batch.to_pylist() instead?
    data = batch.to_pydict()

    # TODO: use Abstract Base Classes to make this easy to extend

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

    with engine.connect() as conn:
        conn.execute(upsert_stmt)

    now = time()

    file_age_s = now - parsed_filename["end_timestamp"]

    last_updated_at = rows[-1]["updated_at"]

    row_age_s = now - last_updated_at.timestamp()

    if file_age_s > row_age_s:
        LOGGER.warning(
            "bad row age!",
            extra={
                "now": now,
                "file_age_s": file_age_s,
                "row_age_s": row_age_s,
                "last_updated_at": last_updated_at.timestamp(),
            },
        )

    statsd.gauge("parquet_file_age_s", file_age_s, tags=dd_tags)
    statsd.gauge("parquet_row_age_s", row_age_s, tags=dd_tags)
    statsd.increment(
        "num_parquet_rows_imported",
        value=len(batch),
        tags=dd_tags,
    )

    progress_callback(1)

    return (i, file_age_s, row_age_s)
