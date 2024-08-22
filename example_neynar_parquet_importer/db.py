import json
import pyarrow.parquet as pq
from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

from example_neynar_parquet_importer.app import LOGGER, PROGRESS_CHUNKS_LOCK

# TODO: detect this from the table
# arrays and json columns are stored as json in parquet because that was easier than dealing with the schema
JSON_COLUMNS = [
    "embeds",
    "mentions",
    "mentions_positions",
    "verified_addresses",
]


def init_db(uri, pool_size):
    engine = create_engine(uri, pool_size=pool_size, max_overflow=0)

    LOGGER.info("migrating...")
    with engine.connect() as connection:
        # TODO: how should we set a custom schema?
        with open("schema.sql", "r") as f:
            connection.execute(text(f.read()))

        # TODO: honestly not sure why i need this. i thought it would auto commit
        connection.commit()

    LOGGER.info("migrations complete.")

    return engine


def check_import_status(engine, file_key, check_last=False):
    with engine.connect() as connection:
        query = "SELECT * FROM parquet_import_tracking WHERE file_key = :file_key ORDER BY imported_at DESC"
        result = connection.execute(text(query), {"file_key": f"{file_key}"}).fetchone()
        if check_last:
            return result["imported_at"] if result else None
        return result is not None


def clean_parquet_data(col, value):
    if col in JSON_COLUMNS:
        return json.loads(value)
    return value


def import_parquet(engine, table_name, local_filename, progress, progress_id):
    assert table_name in local_filename

    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    conn = engine.connect()

    # TODO: open a transaction with automatic rollback on error

    # TODO: save this file into the tracking table and save the id for later

    parquet_file = pq.ParquetFile(local_filename)

    # Get the number of row groups in the file
    num_row_groups = parquet_file.num_row_groups

    # update the progress counter with our new step total
    with PROGRESS_CHUNKS_LOCK:
        new_total = progress.tasks[progress_id].total + num_row_groups

        LOGGER.debug(
            "New total steps for %s %s: %s", progress_id, table_name, f"{new_total:_}"
        )

        progress.update(progress_id, total=new_total)

    primary_key_columns = table.primary_key.columns.values()

    # Read the data in chunks
    # TODO: pretty progress bar here
    for i in range(num_row_groups):
        LOGGER.info(
            "Upsert #%s/%s for %s", f"{i+1:_}", f"{num_row_groups:_}", table_name
        )

        batch = parquet_file.read_row_group(i)

        data = batch.to_pydict()

        # TODO: add the tracking table id to the data

        # collect into a different dict so that we can remove dupes
        rows = {
            tuple(
                clean_parquet_data(col.name, data[col.name][i])
                for col in primary_key_columns
            ): {col: clean_parquet_data(col, data[col][i]) for col in data}
            for i in range(len(batch))
        }

        # discard the keys
        rows = list(rows.values())

        if len(batch) > len(rows):
            LOGGER.debug(
                "Dropping %s rows with duplicate primary keys", len(batch) - len(rows)
            )

        stmt = pg_insert(table).values(rows)

        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=primary_key_columns,
            set_={col: stmt.excluded[col] for col in data.keys()},
        )

        conn.execute(upsert_stmt)

        conn.commit()

        progress.update(progress_id, advance=1)
