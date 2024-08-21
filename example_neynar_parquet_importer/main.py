import logging
import os
from dotenv import load_dotenv
from ipdb import launch_ipdb_on_exception

from example_neynar_parquet_importer.db import import_parquet, init_db
from example_neynar_parquet_importer.s3 import download_latest_full

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


def main():
    logging.info("Hello, World!")

    # TODO V0: set up database
    db_engine = init_db(os.getenv("DATABASE_URI"))

    # TODO: start with just one table, but eventually do all of them (in parallel)
    for table_name in TABLES:
        # TODO V0: check database to see if we've already imported a full. also allow forcing with a flag
        no_full_export = True

        # TODO V0: if no full export, download the latest one
        if no_full_export:
            full_filename = download_latest_full(table_name)

            import_parquet(db_engine, table_name, full_filename)

        # TODO V0: check database to see if we've already imported an incremental that is newer than this full. also allow forcing with a flag

        # TODO V1: subscribe to the SNS topic

        # TODO V0: download all the incrementals between the last import and the current timestamp

    # TODO V1: read from the SNS topic (unless they are older than our full. then we should ignore them)

    logging.info("Success")


if __name__ == "__main__":
    load_dotenv()

    # TODO: check env var to enable json logging
    # TODO: INFO level for s3transfer
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
    )

    logging.getLogger("s3transfer").setLevel(logging.INFO)
    logging.getLogger("botocore").setLevel(logging.INFO)
    logging.getLogger("urllib3").setLevel(logging.INFO)

    if os.getenv("DEBUG") == "true":
        with launch_ipdb_on_exception():
            main()
    else:
        main()
