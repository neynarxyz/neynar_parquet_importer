import json
import os
import logging
import pyarrow.parquet as pq
from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

JSON_COLUMNS = [
    "embeds",
    "mentions",
    "mentions_positions",
]


def init_db(uri):
    # TODO: how do we set a custom schema?
    engine = create_engine(uri)

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

    # Read the data in chunks
    for i in range(num_row_groups):
        logging.info(
            "upsert #%s/%s for %s...", f"{i+1:_}", f"{num_row_groups:_}", table_name
        )

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
