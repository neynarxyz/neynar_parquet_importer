import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import ExitStack
import dotenv
from ipdb import launch_ipdb_on_exception
from rich.live import Live
from rich.panel import Panel
from rich.progress import (
    Progress,
    MofNCompleteColumn,
    DownloadColumn,
    TransferSpeedColumn,
)
from rich.table import Table
from sqlalchemy import update

from .progress import ProgressCallback
from .db import (
    check_for_existing_full_import,
    check_for_existing_incremental_import,
    get_table,
    import_parquet,
    init_db,
)
from .logger import setup_logging
from .s3 import (
    download_incremental,
    download_latest_full,
    get_s3_client,
    parse_parquet_filename,
)
from .settings import Settings

LOGGER = logging.getLogger("app")

# TODO: fetch this from env? tools to generate this list from the s3 bucket?
# NOTE: "messages" is very large and so is not part of parquet exports
ALL_TABLES = {
    ("public-postgres", "farcaster"): [
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
    ],
    ("public-postgres", "nindexer"): [
        "verifications",
    ],
}


def sync_parquet_to_db(
    db_engine,
    file_executor,
    table_name,
    progress_callbacks,
    settings: Settings,
):
    """Function that runs forever (barring exceptions) to download and import parquet files for a table.

    TODO: run downloads and imports in parallel to improve initial sync times
    """
    last_import_filename = None
    parquet_import_tracking = get_table(db_engine, "parquet_import_tracking")
    s3_client = get_s3_client(settings)

    incremental_filename = check_for_existing_incremental_import(
        db_engine, settings, table_name
    )
    if incremental_filename:
        # if we have imported an incremental, then we should start there instead of at the full
        # TODO: if this is very old, we might want to start with a full instead
        LOGGER.debug("Found existing incremental import %s", incremental_filename)

        if not os.path.exists(incremental_filename):
            last_start_timestamp = parse_parquet_filename(incremental_filename)[
                "start_timestamp"
            ]

            incremental_filename = download_incremental(
                s3_client,
                settings,
                table_name,
                last_start_timestamp,
                progress_callbacks["incremental_bytes"],
                progress_callbacks["empty_steps"],
            )

        if incremental_filename:
            import_parquet(
                db_engine,
                table_name,
                incremental_filename,
                "incremental",
                progress_callbacks["incremental_steps"],
                progress_callbacks["empty_steps"],
                settings,
            )

            last_import_filename = incremental_filename

    if last_import_filename is None:
        # no incrementals yet (or a very old one), start with the newest full file
        full_filename = check_for_existing_full_import(db_engine, settings, table_name)

        # TODO: spawn this so we can check for incrementals while this is downloading
        # TODO: need to be sure we handle "mark_completed" properly
        if full_filename is None or not os.path.exists(full_filename):
            # if no full export, download the latest one
            full_filename = download_latest_full(
                s3_client, settings, table_name, progress_callbacks["full_bytes"]
            )

        import_parquet(
            db_engine,
            table_name,
            full_filename,
            "full",
            progress_callbacks["full_steps"],
            progress_callbacks["empty_steps"],
            settings,
        )

        last_import_filename = full_filename

    next_start_timestamp = parse_parquet_filename(last_import_filename)["end_timestamp"]
    next_end_timestamp = next_start_timestamp + settings.incremental_duration

    # download all the incrementals. loops forever
    fs = []
    while True:
        while fs:
            if fs[0].done():
                f = fs.pop(0)

                incremental_filename = f.result()

                mark_completed(db_engine, parquet_import_tracking, incremental_filename)
            else:
                break

        if len(fs) >= settings.s3_pool_size * 2:
            # # this will keep RAM for going super high
            # LOGGER.debug(
            #     "backpressure",
            #     extra={
            #         "table": table_name,
            #         "next_end_timestamp": next_end_timestamp,
            #     },
            # )
            time.sleep(0.1)

        now = time.time()

        if now < next_end_timestamp:
            LOGGER.info(
                "Sleeping until the next incremental is ready",
                extra={
                    "table": table_name,
                    "next_end_timestamp": next_end_timestamp,
                    "now": now,
                },
            )
            time.sleep(next_end_timestamp - now)

        # TODO: spawn a task on file_executor here
        # TODO: have an executor for s3 and another for db?
        # TODO: how should we handle gaps when we start? i think we need a thread that saves the next start row. have a channel of oneshots?
        f = file_executor.submit(
            download_and_import_incremental_parquet,
            db_engine,
            s3_client,
            table_name,
            next_start_timestamp,
            progress_callbacks,
            settings,
        )
        fs.append(f)

        next_start_timestamp += settings.incremental_duration
        next_end_timestamp += settings.incremental_duration


def mark_completed(db_engine, parquet_import_tracking, filename):
    stmt = (
        update(parquet_import_tracking)
        .where(parquet_import_tracking.c.file_name == filename)
        .values(completed=True)
    )

    with db_engine.connect() as conn:
        conn.execute(stmt)

    LOGGER.debug("completed", extra={"file": filename})


def download_and_import_incremental_parquet(
    db_engine,
    s3_client,
    table_name,
    next_start_timestamp,
    progress_callbacks,
    settings: Settings,
):
    incremental_filename = None

    while incremental_filename is None:
        incremental_filename = download_incremental(
            s3_client,
            settings,
            table_name,
            next_start_timestamp,
            progress_callbacks["incremental_bytes"],
            progress_callbacks["empty_steps"],
        )

        if incremental_filename is None:
            LOGGER.debug(
                "Next incremental for %s should be ready soon. Sleeping...", table_name
            )
            # TODO: how long should we sleep? polling isn't great, but SNS seems inefficient with a bunch of tables and short durations
            time.sleep(settings.incremental_duration / 2.0)

    import_parquet(
        db_engine,
        table_name,
        incremental_filename,
        "incremental",
        progress_callbacks["incremental_steps"],
        progress_callbacks["empty_steps"],
        settings,
    )

    return incremental_filename


def main(settings: Settings):
    if settings.tables:
        tables = settings.tables.split(",")
    else:
        tables = ALL_TABLES[(settings.parquet_s3_database, settings.parquet_s3_schema)]

    LOGGER.info("Tables: %s", ",".join(tables))

    db_engine = init_db(str(settings.postgres_dsn), tables, settings)

    target_dir = settings.target_dir()
    if not target_dir.exists():
        target_dir.mkdir(parents=True)

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
        if settings.log_format != "json":
            stack.enter_context(Live(progress_table, refresh_per_second=10))
            enable_progress = True
        else:
            enable_progress = False

        progress_callbacks = {
            "full_bytes": ProgressCallback(
                bytes_progress, "Full", 0, enabled=enable_progress, value_type="Q"
            ),
            "incremental_bytes": ProgressCallback(
                bytes_progress,
                "Incremental",
                0,
                enabled=enable_progress,
                value_type="Q",
            ),
            "full_steps": ProgressCallback(
                steps_progress, "Full", 0, enabled=enable_progress
            ),
            "incremental_steps": ProgressCallback(
                steps_progress, "Incremental", 0, enabled=enable_progress
            ),
            "empty_steps": ProgressCallback(
                steps_progress, "Empty", 0, enabled=enable_progress
            ),
            # TODO: more progress here?
        }

        # do all the tables in parallel
        table_executor = stack.enter_context(
            ThreadPoolExecutor(max_workers=len(tables))
        )
        file_executor = stack.enter_context(
            ThreadPoolExecutor(max_workers=settings.postgres_pool_size - 1)
        )

        table_fs = {
            table_executor.submit(
                sync_parquet_to_db,
                db_engine,
                file_executor,
                table_name,
                progress_callbacks,
                settings,
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

                # TODO: do something with `table_result`?
                table_result
            except Exception:
                LOGGER.exception("Table %s failed", table_name)

                # TODO: raise or break?
                raise


if __name__ == "__main__":
    dotenv.load_dotenv(os.getenv("ENV_FILE", ".env"))

    settings = Settings()

    settings.initialize()

    if settings.interactive_debug:
        with launch_ipdb_on_exception():
            main(settings)
    else:
        main(settings)
