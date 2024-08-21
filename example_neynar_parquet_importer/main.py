import logging
import os
from dotenv import load_dotenv
from ipdb import launch_ipdb_on_exception

from example_neynar_parquet_importer.db import import_full, init_db
from example_neynar_parquet_importer.s3 import download_latest_full

INCREMENTAL_SECONDS = 5 * 60

# TODO: env var to choose the tables that we care about
TABLES = [
    "casts",
    "fids",
    "fnames",
    "links",
    "messages",
    "reactions",
    "signers",
    "storage",
    "user_data",
    "verifications",
    "warpcast_power_users",
    # # TODO: this is a view, so we might not need to import it depending on the target db
    # "profile_with_addresses",
]


def main():
    logging.info("Hello, World!")

    # TODO: start with just one table, but eventually do all of them (in parallel)
    table_name = "fids"

    # TODO V0: set up database
    db_engine = init_db(os.getenv("DATABASE_URI"))

    # TODO V0: check database to see if we've already imported a full. also allow forcing with a flag
    no_full_export = True

    # TODO V0: if no full export, download the latest one
    if no_full_export:
        full_filename = download_latest_full(table_name)

        import_full(db_engine, table_name, full_filename)


    # TODO V0: check database to see if we've already imported an incremental that is newer than this full. also allow forcing with a flag

    # TODO V1: subscribe to the SNS topic

    # TODO V0: download all the incrementals between the last import and the current timestamp

    # TODO V1: read from the SNS topic (unless they are older than our full. then we should ignore them)


if __name__ == "__main__":
    load_dotenv()

    # TODO: check env var to enable json logging
    # TODO: INFO level for s3transfer
    logging.basicConfig(level=logging.DEBUG)

    with launch_ipdb_on_exception():
        main()
