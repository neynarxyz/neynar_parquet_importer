import json
import logging
import pyarrow.parquet as pq
import time
from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

JSON_COLUMNS = [
    "embeds",
    "mentions",
    "mentions_positions",
]


def init_db(uri, pool_size):
    # TODO: how do we set a custom schema?
    engine = create_engine(uri, pool_size=pool_size, max_overflow=0)

    logging.info("migrating...")
    with engine.connect() as connection:
        with open("schema.sql", "r") as f:
            connection.execute(text(f.read()))

        # TODO: honestly not sure why i need this. i thought it would auto commit
        connection.commit()

    logging.info("migrations complete.")

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


def humanize_time(seconds):
    if seconds < 60:
        return f"{int(seconds)}s"
    elif seconds < 3600:
        minutes = int(seconds // 60)
        return f"{minutes}m"
    else:
        hours = int(seconds // 3600)
        minutes = int((seconds % 3600) // 60)
        return f"{hours}h {minutes}m"


def import_parquet(engine, table_name, local_filename):
    assert table_name in local_filename

    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    conn = engine.connect()

    # TODO: open a transaction with automatic rollback on error

    # TODO: save this file into the tracking table and save the id for later

    parquet_file = pq.ParquetFile(local_filename)

    # Get the number of row groups in the file
    num_row_groups = parquet_file.num_row_groups

    start_time = time.time()

    # Read the data in chunks
    # TODO: pretty progress bar here
    for i in range(num_row_groups):
        # logging.info(
        #     "Upserting #%s/%s for %s", f"{i+1:_}", f"{num_row_groups:_}", table_name
        # )

        batch = parquet_file.read_row_group(i)

        data = batch.to_pydict()

        # TODO: add the tracking table id to the data

        rows = [
            {col: clean_parquet_data(col, data[col][i]) for col in data}
            for i in range(len(batch))
        ]

        stmt = pg_insert(table).values(rows)

        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=table.primary_key.columns.values(),
            set_={col: stmt.excluded[col] for col in data.keys()},
        )

        conn.execute(upsert_stmt)

        conn.commit()

        elapsed_time = time.time() - start_time
        average_time_per_group = elapsed_time / (i + 1)
        remaining_groups = num_row_groups - (i + 1)
        estimated_total_time = average_time_per_group * num_row_groups
        estimated_time_remaining = average_time_per_group * remaining_groups

        elapsed_time_human = humanize_time(elapsed_time)
        estimated_total_time_human = humanize_time(estimated_total_time)
        estimated_time_remaining_human = humanize_time(estimated_time_remaining)

        # TODO: humanize the seconds
        logging.info(
            "Upsert #%s/%s for %s. Elapsed: %s, Total: ~%s, Remaining: ~%s",
            f"{i+1:_}",
            f"{num_row_groups:_}",
            table_name,
            elapsed_time_human,
            estimated_total_time_human,
            estimated_time_remaining_human,
        )
