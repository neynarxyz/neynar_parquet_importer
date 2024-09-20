from contextlib import ExitStack
import logging
import os
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dotenv import load_dotenv
from ipdb import launch_ipdb_on_exception
from rich.logging import RichHandler
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    Progress,
    MofNCompleteColumn,
    DownloadColumn,
    TransferSpeedColumn,
)
from rich.table import Table

from .app import PROGRESS_BYTES_LOCK, PROGRESS_CHUNKS_LOCK, ProgressCallback
from .db import (
    check_for_existing_full_import,
    check_for_existing_incremental_import,
    import_parquet,
    init_db,
)
from .s3 import (
    download_incremental,
    download_latest_full,
)

LOGGER = logging.getLogger("app")
INCREMENTAL_SECONDS = 5 * 60

# NOTE: "messages" is very large and so is not part of parquet exports
ALL_TABLES = [
    "blocks",
    "casts",
    "channel_follows",
    "fids",
    "fnames",
    "links",
    "power_users",
    "profile_with_addresses",
    "reactions",
    "signers",
    "storage",
    "user_data",
    "verifications",
    "warpcast_power_users",
]


def sync_parquet_to_db(
    db_engine,
    table_name,
    full_bytes_downloaded_progress,
    incremental_bytes_downloaded_progress,
    full_steps_progress,
    incremental_steps_progress,
):
    """Function that runs forever (barring exceptions) to download and import parquet files for a table.

    TODO: run downloads and imports in parallel to improve initial sync times
    """
    incremental_filename = check_for_existing_incremental_import(db_engine, table_name)
    if incremental_filename:
        # if we have imported an incremental, then we should start there instead of at the full
        LOGGER.info("Found existing incremental import %s", incremental_filename)

        import_parquet(
            db_engine,
            table_name,
            incremental_filename,
            "incremental",
            incremental_steps_progress,
        )

        last_import_filename = incremental_filename
    else:
        # no incrementals yet, start with the newest full file
        full_filename = check_for_existing_full_import(db_engine, table_name)

        if full_filename is None:
            # if no full export, download the latest one
            full_filename = download_latest_full(
                table_name, full_bytes_downloaded_progress
            )

        import_parquet(
            db_engine, table_name, full_filename, "full", full_steps_progress
        )

        last_import_filename = full_filename

    # TODO: check the database to see if we've already imported incrementals
    # TODO: when writing more advanced logic for skipping handled files, be sure not to miss any! upgrades or outages might cause a file to be missing for a couple hours
    match = re.match(r"(.+)-(.+)-(\d+)-(\d+)\.parquet", last_import_filename)
    if match:
        # db_name = match.group(1)
        # table_name = match.group(2)
        # start_timestamp = match.group(3)
        next_start_timestamp = int(match.group(4))
    else:
        raise ValueError(
            "Last Import filename does not match expected format.", last_import_filename
        )

    # TODO: subscribe to the SNS topic and read from it instead of polling

    # download all the incrementals. loops forever
    while True:
        now = int(time.time())

        if now < next_start_timestamp:
            LOGGER.info("Sleeping until the next incremental is ready")
            time.sleep(next_start_timestamp - now)

        incremental_filename = download_incremental(
            table_name,
            next_start_timestamp,
            INCREMENTAL_SECONDS,
            incremental_bytes_downloaded_progress,
        )

        if incremental_filename is None:
            LOGGER.debug(
                "Next incremental for %s should be ready soon. Sleeping...", table_name
            )
            time.sleep(60)
            continue

        next_start_timestamp += INCREMENTAL_SECONDS

        if incremental_filename.endswith(".empty"):
            continue

        import_parquet(
            db_engine,
            table_name,
            incremental_filename,
            "incremental",
            incremental_steps_progress,
        )


def main():
    tables = os.getenv("TABLES")

    if tables:
        tables = tables.split(",")
    else:
        tables = ALL_TABLES

    LOGGER.info("Tables: %s", ",".join(tables))

    # connect to and set up the database
    pool_size = len(tables) * 2

    db_engine = init_db(os.getenv("DATABASE_URI"), pool_size)

    with ExitStack() as stack:
        # these pretty progress bars show when you run the application in an interactive terminal
        bytes_progress = Progress(
            *Progress.get_default_columns(),
            DownloadColumn(),
            TransferSpeedColumn(),
            refresh_per_second=10,
        )
        steps_progress = Progress(
            *Progress.get_default_columns(),
            MofNCompleteColumn(),
            refresh_per_second=10,
        )

        progress_table = Table.grid()
        progress_table.add_row(
            Panel.fit(
                bytes_progress,
                title="Bytes Downloaded",
                border_style="green",
                padding=(0, 0),
            ),
            Panel.fit(
                steps_progress,
                title="Steps Imported",
                border_style="blue",
                padding=(0, 0),
            ),
        )

        # this only shows in a terminal. it does not show in docker logs
        # TODO: send a periodic log message to show progress in docker logs
        stack.enter_context(Live(progress_table, refresh_per_second=10))

        full_bytes_downloaded_id = bytes_progress.add_task("Full", total=0)
        incremental_bytes_downloaded_id = bytes_progress.add_task(
            "Incremental", total=0
        )
        full_steps_id = steps_progress.add_task("Full", total=0)
        incremental_steps_id = steps_progress.add_task("Incremental", total=0)

        full_bytes_callback = ProgressCallback(
            bytes_progress, full_bytes_downloaded_id, 0, PROGRESS_BYTES_LOCK
        )
        incremental_bytes_callback = ProgressCallback(
            bytes_progress, incremental_bytes_downloaded_id, 0, PROGRESS_BYTES_LOCK
        )

        full_steps_callback = ProgressCallback(
            steps_progress, full_steps_id, 0, PROGRESS_CHUNKS_LOCK
        )
        incremental_steps_callback = ProgressCallback(
            steps_progress, incremental_steps_id, 0, PROGRESS_CHUNKS_LOCK
        )

        # do all the tables in parallel
        table_executor = stack.enter_context(
            ThreadPoolExecutor(max_workers=len(tables))
        )

        table_fs = {
            table_executor.submit(
                sync_parquet_to_db,
                db_engine,
                table_name,
                full_bytes_callback,
                incremental_bytes_callback,
                full_steps_callback,
                incremental_steps_callback,
            ): table_name
            for table_name in tables
        }

        # wait for importers to finish
        # they will run forever, so this really only needs to handle exceptions
        for future in as_completed(table_fs):
            table_name = table_fs[future]

            LOGGER.warning("Table %s finished. This is unexpected", table_name)

            # this will raise an excecption if `sync_parquet_to_db` failed
            # the result should always be ready. no timeout is needed
            try:
                table_result = future.result()
            except Exception:
                LOGGER.exception("Table %s failed", table_name)

                # TODO: raise or break?
                raise


if __name__ == "__main__":
    load_dotenv()

    # TODO: check env var to enable json logging
    logging.basicConfig(
        level=logging.NOTSET,
        format="%(name)s - %(message)s",
        datefmt="%X",
        handlers=[RichHandler()],
    )

    # TODO: env vars to control logging
    logging.getLogger("app").setLevel(logging.INFO)
    logging.getLogger("s3transfer").setLevel(logging.INFO)
    logging.getLogger("boto3").setLevel(logging.INFO)
    logging.getLogger("botocore").setLevel(logging.INFO)
    logging.getLogger("urllib3").setLevel(logging.INFO)

    if os.getenv("INTERACTIVE_DEBUG") == "true":
        with launch_ipdb_on_exception():
            main()
    else:
        main()
