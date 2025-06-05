# TODO: take a /Users/bryan/neynar/neynar_parquet_exporter/data/v3/public-postgres/nindexer-casts-0-1744320248.parquet
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
from datetime import datetime
import logging
import os
from os import PathLike
from pathlib import Path
import time
import dotenv
from rich.progress import (
    Progress,
    MofNCompleteColumn,
    DownloadColumn,
    TransferSpeedColumn,
)
from ipdb import launch_ipdb_on_exception

from neynar_parquet_importer.db import get_tables, import_parquet, init_db
from neynar_parquet_importer.progress import ProgressCallback
from neynar_parquet_importer.s3 import parse_parquet_filename, download_known_full, get_s3_client
from neynar_parquet_importer.settings import SHUTDOWN_EVENT, CuMode, Settings


def main(
    settings: Settings,
    parquet_file: PathLike,
    start_timestamp: int,
    end_timestamp: int,
):
    with ExitStack() as stack:
        shutdown_executor = row_group_executor = None
        try:
            parsed_filename = parse_parquet_filename(parquet_file)

            table_name = parsed_filename["table_name"]

            table_names = [
                table_name,
            ]

            db_engine = init_db(str(settings.postgres_dsn), table_names, settings)

            tables = get_tables(settings.postgres_schema, db_engine, [])

            try:
                table = tables[table_name]
            except KeyError:
                raise KeyError(
                    f"Table {table_name} not found in schema {settings.postgres_schema}",
                    tables.keys(),
                )
            parquet_import_tracking = tables["parquet_import_tracking"]

            # TODO: finish the progress bars
            steps_progress = Progress(
                *Progress.get_default_columns(),
                MofNCompleteColumn(),
                refresh_per_second=10,
            )
            bytes_progress = Progress(
                *Progress.get_default_columns(),
                DownloadColumn(),
                TransferSpeedColumn(),
                refresh_per_second=10,
            )
            progress_callback = ProgressCallback(
                steps_progress, "files", 0, enabled=False
            )
            progress_callback_full_bytes = ProgressCallback(
                bytes_progress,
                "full_bytes",
                0,
                enabled=True,
                value_type="I",
            )
            empty_callback = ProgressCallback(steps_progress, "empty", 0, enabled=False)

            row_group_executor = stack.enter_context(
                ThreadPoolExecutor(
                    max_workers=settings.row_workers,
                    thread_name_prefix=f"{table_name}Rows",
                )
            )
            shutdown_executor = stack.enter_context(
                ThreadPoolExecutor(
                    max_workers=1,
                    thread_name_prefix="Shutdown",
                )
            )

            download_threadpool = ThreadPoolExecutor(
                max_workers=1,
                thread_name_prefix="BackfillDownload",
            )

            # TODO: move this to a helper
            f_shutdown = shutdown_executor.submit(SHUTDOWN_EVENT.wait)

            # TODO: load filters from an optional file
            row_filters = []

            s3_client = get_s3_client(settings)

            # future TODO: decide whether to pull latest based on start/end timestamps
            full_filename = download_known_full(
                download_threadpool,
                s3_client,
                settings,
                parquet_file,
                progress_callback_full_bytes,
            )

            if end_timestamp == 0:
                end_timestamp = parsed_filename.get("end_timestamp", int(time.time()))

            x = import_parquet(
                db_engine,
                table,
                full_filename,
                'full',
                progress_callback,
                empty_callback,
                parquet_import_tracking,
                row_group_executor,
                row_filters,
                settings,
                f_shutdown,
                backfill=True,  # should backfill
                backfill_start_timestamp=datetime.fromtimestamp(start_timestamp),
                backfill_end_timestamp=datetime.fromtimestamp(end_timestamp),
            )

            logging.info("", extra={"x": x})

            # TODO: how should we handle "mark completed?"
        finally:
            SHUTDOWN_EVENT.set()

            if row_group_executor:
                row_group_executor.shutdown(wait=False, cancel_futures=True)
            else:
                logging.error("Row group executor is None")

            if shutdown_executor:
                shutdown_executor.shutdown(wait=False, cancel_futures=True)
            else:
                logging.error("Shutdown executor is None")


if __name__ == "__main__":
    dotenv.load_dotenv(os.getenv("ENV_FILE", ".env"))

    settings = Settings()

    settings.cu_mode = CuMode.SHADOW

    settings.initialize()

    # TODO: more pydantic?
    parquet_file = Path(os.environ["PARQUET_FILE"])
    start_timestamp = os.environ.get("START_TIMESTAMP", None)
    end_timestamp = int(os.environ.get("END_TIMESTAMP", 0))
    if start_timestamp is None:
        raise ValueError("No START_TIMESTAMP set")
    if end_timestamp == 0:
        logging.warning(
            "No END_TIMESTAMP set, defaulting to 0 (now). This may not be what you want."
        )

    # if not parquet_file.exists():
    #    raise ValueError(f"Parquet file {parquet_file} does not exist")

    if settings.interactive_debug:
        with launch_ipdb_on_exception():
            main(settings, parquet_file, start_timestamp=int(start_timestamp), end_timestamp=end_timestamp)
    else:
        main(settings, parquet_file, start_timestamp=int(start_timestamp), end_timestamp=end_timestamp)
