import logging
import os
import re
import time
from dotenv import load_dotenv
from ipdb import launch_ipdb_on_exception

from example_neynar_parquet_importer.db import import_parquet, init_db
from example_neynar_parquet_importer.s3 import (
    download_incremental,
    download_latest_full,
)

INCREMENTAL_SECONDS = 5 * 60

# TODO: env var to choose the tables that we care about
# TODO: messages and reactions are not part of parquet exports
TABLES = [
    # "casts",
    "fids",
    # "fnames",
    # "links",
    # "signers",
    # "storage",
    # "user_data",
    # "verifications",
    # "warpcast_power_users",
    # # TODO: this is a view, so we might not need to import it depending on the target db
    # "profile_with_addresses",
]


def main():
    # connect to and set up the database
    db_engine = init_db(os.getenv("DATABASE_URI"))

    # TODO: do these all in parallel. now that we loop `while True`, this only runs for the first table
    for table_name in TABLES:
        # TODO V0: check database to see if we've already imported a full. also allow forcing with a flag
        full_filename = None

        # if no full export, download the latest one
        if full_filename is None:
            full_filename = download_latest_full(table_name)

            # # TODO: turn this back on once done debugging
            # import_parquet(db_engine, table_name, full_filename)

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
                logging.debug("Next incremental should be ready soon. Sleeping...")
                time.sleep(60)
                continue

            next_start_timestamp += INCREMENTAL_SECONDS

            if incremental_filename.endswith(".empty"):
                logging.debug("File was empty: %s", incremental_filename)
                continue

            import_parquet(db_engine, table_name, incremental_filename)

    logging.info("Success")


if __name__ == "__main__":
    load_dotenv()

    # TODO: check env var to enable json logging
    # TODO: INFO level for s3transfer
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s %(name)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
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
