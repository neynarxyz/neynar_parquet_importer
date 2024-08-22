from contextlib import ExitStack
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from ipdb import launch_ipdb_on_exception
from rich.logging import RichHandler

from example_neynar_parquet_importer.db import import_parquet, init_db
from example_neynar_parquet_importer.s3 import (
    download_incremental,
    download_latest_full,
)

INCREMENTAL_SECONDS = 5 * 60

# TODO: env var to choose the tables that we care about
# TODO: messages and reactions are not part of parquet exports
TABLES = [
    "casts",
    "fids",
    "fnames",
    "links",
    "signers",
    "storage",
    "user_data",
    "verifications",
    "warpcast_power_users",
    # # TODO: this is a view, so we might not need to import it depending on the target db
    # "profile_with_addresses",
]


def sync_parquet_to_db(db_engine, table_name):
    """Function that runs forever (barring exceptions) to download and import parquet files for a table.

    TODO: run downloads and imports in parallel to improve initial sync times
    """

    # TODO V0: check database to see if we've already imported a full. also allow forcing with a flag
    full_filename = None

    # if no full export, download the latest one
    if full_filename is None:
        full_filename = download_latest_full(table_name)

        import_parquet(db_engine, table_name, full_filename)

        match = re.match(r"(.+)-(.+)-(\d+)-(\d+)\.parquet", full_filename)
        if match:
            # db_name = match.group(1)
            # table_name = match.group(2)
            # start_timestamp = match.group(3)
            next_start_timestamp = int(match.group(4))
        else:
            raise ValueError(
                "Full filename does not match expected format.", full_filename
            )
    else:
        # TODO V0: check database to see if we've already imported an incremental that is newer than this full. also allow forcing with a flag

        next_start_timestamp = NotImplemented

    # TODO V1: subscribe to the SNS topic and read from it instead of polling

    # download all the incrementals. loops forever
    while True:
        now = int(time.time())

        if now < next_start_timestamp:
            logging.info("Sleeping until the next incremental is ready")
            time.sleep(next_start_timestamp - now)

        incremental_filename = download_incremental(
            table_name, next_start_timestamp, INCREMENTAL_SECONDS
        )

        if incremental_filename is None:
            logging.debug(
                "Next incremental for %s should be ready soon. Sleeping...", table_name
            )
            time.sleep(60)
            continue

        next_start_timestamp += INCREMENTAL_SECONDS

        if incremental_filename.endswith(".empty"):
            continue

        import_parquet(db_engine, table_name, incremental_filename)


def main():
    # connect to and set up the database
    db_engine = init_db(os.getenv("DATABASE_URI"), len(TABLES))

    with ExitStack() as stack:
        table_executor = stack.enter_context(
            ThreadPoolExecutor(max_workers=len(TABLES))
        )

        table_fs = {
            table_executor.submit(
                sync_parquet_to_db,
                db_engine,
                table_name,
            ): table_name
            for table_name in TABLES
        }

        # wait for functions to finish
        # they will run forever, so this really only needs to handle exceptions
        for future in as_completed(table_fs):
            table_name = table_fs[future]

            logging.warn("Table %s finished. This is unexpected", table_name)

            # this will raise an excecption if `sync_parquet_to_db` failed
            # the result should always be ready. no timeout is needed
            table_result = future.result()

    logging.info("Success")


if __name__ == "__main__":
    load_dotenv()

    # TODO: check env var to enable json logging
    # TODO: INFO level for s3transfer
    logging.basicConfig(
        level=logging.INFO, format="%(message)s", datefmt="%X", handlers=[RichHandler()]
    )

    logging.getLogger("s3transfer").setLevel(logging.INFO)
    logging.getLogger("boto3").setLevel(logging.INFO)
    logging.getLogger("botocore").setLevel(logging.INFO)
    logging.getLogger("urllib3").setLevel(logging.INFO)

    if os.getenv("DEBUG") == "true":
        with launch_ipdb_on_exception():
            main()
    else:
        main()
