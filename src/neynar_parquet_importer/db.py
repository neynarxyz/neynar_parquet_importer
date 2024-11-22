import json
import pyarrow.parquet as pq
from sqlalchemy import MetaData, Table, create_engine, select, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from neynar_parquet_importer.app import LOGGER
from neynar_parquet_importer.settings import Settings

# TODO: detect this from the table
# arrays and json columns are stored as json in parquet because that was easier than dealing with the schema
JSON_COLUMNS = [
    "embeds",
    "mentions",
    "mentions_positions",
    "verified_addresses",
]


def init_db(uri, pool_size):
    """Initialize the database with our simple schema."""
    engine = create_engine(uri, pool_size=pool_size, max_overflow=2)

    LOGGER.info("migrating...")
    with engine.connect() as connection:
        # TODO: how should we set a custom schema?
        with open("schema.sql", "r") as f:
            connection.execute(text(f.read()))

        # TODO: honestly not sure why i need this. i thought it would auto commit
        connection.commit()

    LOGGER.info("migrations complete.")

    return engine


def check_for_existing_incremental_import(engine, settings: Settings, table_name):
    """Returns the filename for the newest incremental. This may only be partially imported."""

    # TODO: cache this Table?
    parquet_import_tracking = Table(
        "parquet_import_tracking", MetaData(), autoload_with=engine
    )

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

    parquet_import_tracking = Table(
        "parquet_import_tracking", MetaData(), autoload_with=engine
    )

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


def import_parquet(
    engine,
    table_name,
    local_filename,
    file_type,
    progress_callback,
    settings: Settings,
):
    assert table_name in local_filename

    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    tracking_table = Table("parquet_import_tracking", metadata, autoload_with=engine)

    is_empty = local_filename.endswith(".empty")

    if is_empty:
        num_row_groups = 0
    else:
        parquet_file = pq.ParquetFile(local_filename)
        num_row_groups = parquet_file.num_row_groups

    with engine.connect() as conn:
        # check the database to see if we've already imported this file
        query = select(
            tracking_table.c.id, tracking_table.c.last_row_group_imported
        ).where(tracking_table.c.file_name == local_filename)
        result = conn.execute(query).fetchone()

        if result is None:
            LOGGER.debug("inserting %s into the tracking table", local_filename)

            # do NOT put 0 here. that would mean that we already imported row group 0!
            last_row_group_imported = None

            insert = tracking_table.insert().values(
                table_name=table_name,
                file_name=local_filename,
                file_type=file_type,
                file_version=settings.npe_version,
                file_duration_s=settings.incremental_duration,
                is_empty=is_empty,
                last_row_group_imported=last_row_group_imported,
                total_row_groups=num_row_groups,
            )

            result = conn.execute(insert)

            tracking_id = result.inserted_primary_key[0]
        else:
            (tracking_id, last_row_group_imported) = result

        if is_empty:
            LOGGER.info("Skipping import of empty file %s", local_filename)
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
        for i in range(start_row_group, num_row_groups):
            LOGGER.info(
                "Upsert #%s/%s for %s", f"{i+1:_}", f"{num_row_groups:_}", table_name
            )

            # TODO: larger batches with `pf.iter_batches(batch_size=X)` instead of row groups
            batch = parquet_file.read_row_group(i)

            data = batch.to_pydict()

            # collect into a different dict so that we can remove dupes
            # NOTE: You can modify the data however you want here. Do things like pull values out of json columns or skip columns entirely.
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

            upsert_stmt = stmt.on_conflict_do_update(
                index_elements=primary_key_columns,
                set_={col: stmt.excluded[col] for col in data.keys()},
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
