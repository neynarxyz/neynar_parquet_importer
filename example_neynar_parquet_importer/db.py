import os
import d6tstack
import logging
import pandas as pd
import pyarrow.parquet as pq
from sqlalchemy import MetaData, Table, create_engine, text
from sqlalchemy.dialects.postgresql import insert as pg_insert

CHUNK_SIZE = 10_000

def init_db(uri):
    # TODO: how do we set a custom schema?
    engine = create_engine(uri)

    logging.info("migrating...")
    with engine.connect() as connection:
        with open('schema.sql', 'r') as f:
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
            return result['imported_at'] if result else None
        return result is not None


# TODO: hard code index column?
def import_full(engine, table_name, local_filename):
    assert table_name in local_filename

    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine)

    conn = engine.connect()

    # TODO: open a transaction with automatic rollback on error

    # TODO: save this file into the tracking table and save the id for later

    # TODO: chunk size seems to have some maximum set outside our control
    for (i, batch) in enumerate(pq.read_table(local_filename).to_batches(CHUNK_SIZE)):
        data = batch.to_pydict()

        # TODO: add the tracking table id to the data

        rows = [
            {col: data[col][i] for col in data}
            for i in range(len(data))
        ]

        logging.info("upserting %s #%s...", table_name, i)
    
        stmt = pg_insert(table).values(rows)

        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=table.primary_key.columns.values(),
            set_={col: stmt.excluded[col] for col in data.keys()}
        )

        conn.execute(upsert_stmt)

        conn.commit()
