# TODO: take a /Users/bryan/neynar/neynar_parquet_exporter/data/v3/public-postgres/nindexer-casts-0-1744320248.parquet
from concurrent.futures import ThreadPoolExecutor
from contextlib import ExitStack
import logging
import os
from pathlib import Path
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
from neynar_parquet_importer.s3 import parse_parquet_filename
from neynar_parquet_importer.settings import SHUTDOWN_EVENT, Settings


def main(parquet_file: Path, settings: Settings):
    # TODO: how can we load a single file? how should we handle the tracking table?

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

            if parsed_filename["start_timestamp"] > 0:
                file_type = "incremental"
            else:
                file_type = "full"

            # TODO: finish the progress bars
            steps_progress = Progress(
                *Progress.get_default_columns(),
                MofNCompleteColumn(),
                refresh_per_second=10,
            )
            progress_callback = ProgressCallback(
                steps_progress, "files", 0, enabled=False
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

            # TODO: move this to a helper
            f_shutdown = shutdown_executor.submit(SHUTDOWN_EVENT.wait)

            # TODO: load filters from an optional file
            row_filters = []

            x = import_parquet(
                db_engine,
                table,
                parquet_file,
                file_type,
                progress_callback,
                empty_callback,
                parquet_import_tracking,
                row_group_executor,
                row_filters,
                settings,
                f_shutdown,
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

    settings.initialize()

    # TODO: more pydantic?
    parquet_file = Path(os.environ["PARQUET_FILE"])

    if not parquet_file.exists():
        raise ValueError(f"Parquet file {parquet_file} does not exist")

    if settings.interactive_debug:
        with launch_ipdb_on_exception():
            main(parquet_file, settings)
    else:
        main(parquet_file, settings)
