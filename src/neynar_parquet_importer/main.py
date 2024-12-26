import logging
import os
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import ExitStack
import dotenv
from ipdb import launch_ipdb_on_exception
from psycopg import OperationalError
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
from tenacity import retry, retry_if_exception_type, stop_after_attempt, wait_fixed

from .progress import ProgressCallback
from .db import (
    check_for_existing_full_import,
    check_for_existing_incremental_import,
    get_table,
    import_parquet,
    init_db,
    raise_any_exceptions,
)
from .s3 import (
    download_incremental,
    download_latest_full,
    get_s3_client,
    parse_parquet_filename,
)
from .settings import SHUTDOWN_EVENT, Settings

LOGGER = logging.getLogger("app")


# TODO: fetch this from env? tools to generate this list from the s3 bucket?
# NOTE: "messages" is very large and so is not part of parquet exports
ALL_TABLES = {
    # for these, set `npe_version=v2` `parquet_s3_schema=farcaster` `incremental_duration=300`
    ("public-postgres", "farcaster"): [
        # "account_verifications",  # TODO: schema for this coming soon!
        "blocks",
        "casts",  # NOTE: `casts` is VERY large with LOTS of writes!
        # "channel_follows",  # TODO: schema for this coming soon!
        # "channel_members",  # TODO: schema for this coming soon!
        # "channels",  # TODO: schema for this coming soon!
        "fids",
        "fnames",
        "links",
        # "power_users",  # TODO: schema for this coming soon!
        # "profile_with_addresses",  # TODO: `profile_with_addresses` is a view and needs some special handling for duplicate ids
        "reactions",  # NOTE: `reactions` is VERY large with LOTS of writes!
        "signers",
        "storage",
        "user_data",
        # "verifications",  # NOTE: please use the nindexer verifications table instead
        "warpcast_power_users",
    ],
    # for these, set `npe_version=v3` `parquet_s3_schema=nindexer` `incremental_duration=1`
    ("public-postgres", "nindexer"): [
        # "follow_counts",  # TODO: schema for this coming soon!
        "follows",
        # "neynar_user_scores",  # TODO: schema for this coming soon!
        "profiles",
        "verifications",
    ],
}


def sync_parquet_to_db(
    db_engine,
    file_executor,
    row_group_executor,
    table_name,
    progress_callbacks,
    settings: Settings,
):
    """Function that runs forever (barring exceptions) to download and import parquet files for a table.

    TODO: run downloads and imports in parallel to improve initial sync times
    """
    try:
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
                    row_group_executor,
                    settings,
                )

                last_import_filename = incremental_filename

        if last_import_filename is None:
            # no incrementals yet (or a very old one), start with the newest full file
            full_filename = check_for_existing_full_import(
                db_engine, settings, table_name
            )

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
                row_group_executor,
                settings,
            )

            mark_completed(db_engine, parquet_import_tracking, [full_filename])

            last_import_filename = full_filename

        next_start_timestamp = parse_parquet_filename(last_import_filename)[
            "end_timestamp"
        ]
        next_end_timestamp = next_start_timestamp + settings.incremental_duration

        max_wait_duration = max(60, 4 * settings.incremental_duration)

        # download all the incrementals. loops forever
        fs = []
        while not SHUTDOWN_EVENT.is_set():
            # mark files completed in order. this keeps us from skipping items if we have to restart
            completed_filenames = []
            while fs:
                if fs[0].done():
                    f = fs.pop(0)

                    incremental_filename = f.result()

                    if incremental_filename is None:
                        raise RuntimeError("incremental is None")

                    completed_filenames.append(incremental_filename)
                elif fs[0].cancelled():
                    LOGGER.debug("cancelled")
                    return
                else:
                    break

            raise_any_exceptions(fs)

            mark_completed(db_engine, parquet_import_tracking, completed_filenames)

            # TODO: if fs is super long, what should we do?

            now = time.time()
            if now < next_end_timestamp:
                sleep_amount = next_end_timestamp - now

                # # TODO: this is too verbose
                # LOGGER.debug(
                #     "Sleeping until the next incremental is ready",
                #     extra={
                #         "table": table_name,
                #         "next_end": next_end_timestamp,
                #         "next_start": next_start_timestamp,
                #         "sleep_amount": sleep_amount,
                #     },
                # )

                if SHUTDOWN_EVENT.wait(sleep_amount):
                    LOGGER.debug("Shutting down")
                    return

            # TODO: spawn a task on file_executor here
            # TODO: have an executor for s3 and another for db?
            # TODO: how should we handle gaps when we start? i think we need a thread that saves the next start row. have a channel of oneshots?
            f = file_executor.submit(
                download_and_import_incremental_parquet,
                db_engine,
                s3_client,
                table_name,
                max_wait_duration,
                next_start_timestamp,
                progress_callbacks,
                row_group_executor,
                settings,
            )
            fs.append(f)

            next_start_timestamp += settings.incremental_duration
            next_end_timestamp += settings.incremental_duration
    except Exception as e:
        if e.args == ("cannot schedule new futures after shutdown",):
            LOGGER.debug("Executor is shutting down during sync_parquet_to_db")
            return

        LOGGER.exception("exception inside sync_parquet_to_db")
        SHUTDOWN_EVENT.set()
        raise
    finally:
        # this should run forever. any exit here means we should shut down the whole app
        SHUTDOWN_EVENT.set()


@retry(
    stop=stop_after_attempt(3),
    wait=wait_fixed(0.2),
    retry=retry_if_exception_type(OperationalError),
)
def mark_completed(db_engine, parquet_import_tracking, completed_filenames):
    if not completed_filenames:
        return

    stmt = (
        update(parquet_import_tracking)
        .where(parquet_import_tracking.c.file_name.in_(completed_filenames))
        .values(completed=True)
    )

    with db_engine.connect() as conn:
        conn.execute(stmt)
        conn.commit()

    # this is too verbose
    # LOGGER.debug("completed", extra={"files": completed_filenames})


def download_and_import_incremental_parquet(
    db_engine,
    s3_client,
    table_name,
    max_wait_duration,
    next_start_timestamp,
    progress_callbacks,
    row_group_executor,
    settings: Settings,
):
    # as long as at least one file on this table is progressing, we are okay and shouldn't exit/warn
    # TODO: use a shared watchdog for this table instead of having every import track its own age.
    max_wait = time.time() + max_wait_duration

    incremental_filename = None
    try:
        while incremental_filename is None:
            if SHUTDOWN_EVENT.is_set():
                return

            now = time.time()
            if now > max_wait:
                extra = {
                    "max_wait_duration": max_wait_duration,
                    "table_name": table_name,
                    "next_start_timestamp": next_start_timestamp,
                    "now": now,
                }
                if settings.exit_after_max_wait:
                    # this is a sledge hammer. think more about this!
                    raise ValueError("Max wait exceeded", extra)
                else:
                    LOGGER.warning("Max wait exceeded", extra=extra)

            incremental_filename = download_incremental(
                s3_client,
                settings,
                table_name,
                next_start_timestamp,
                progress_callbacks["incremental_bytes"],
                progress_callbacks["empty_steps"],
            )

            if incremental_filename is None:
                # TODO: how long should we sleep? polling isn't great, but SNS seems inefficient with a bunch of tables and short durations
                sleep_amount = min(30, settings.incremental_duration / 2.0)

                extra = {
                    "table_name": table_name,
                    "sleep_amount": sleep_amount,
                    "start_timestamp": next_start_timestamp,
                }

                LOGGER.debug(
                    "This incremental should be ready soon. Sleeping",
                    extra=extra,
                )

                if SHUTDOWN_EVENT.wait(sleep_amount):
                    LOGGER.debug("Shutting down", extra=extra)
                    return

        import_parquet(
            db_engine,
            table_name,
            incremental_filename,
            "incremental",
            progress_callbacks["incremental_steps"],
            progress_callbacks["empty_steps"],
            row_group_executor,
            settings,
        )

        # we got a file. reset max wait
        if max_wait_duration:
            max_wait = time.time() + max_wait_duration
    except Exception as e:
        if e.args == ("cannot schedule new futures after shutdown",):
            LOGGER.debug("Executor is shutting down during sync_parquet_to_db")
            return

        LOGGER.exception("exception inside download_and_import_incremental_parquet")
        SHUTDOWN_EVENT.set()
        raise

    return incremental_filename


def main(settings: Settings):
    with ExitStack() as stack:
        db_engine = table_executor = file_executor = row_group_executors = None
        try:
            if settings.tables:
                tables = settings.tables.split(",")
            else:
                tables = ALL_TABLES[
                    (settings.parquet_s3_database, settings.parquet_s3_schema)
                ]

            LOGGER.info("Tables: %s", ",".join(tables))

            db_engine = init_db(str(settings.postgres_dsn), tables, settings)

            # TODO: test the s3 client here?

            target_dir = settings.target_dir()
            if not target_dir.exists():
                target_dir.mkdir(parents=True)

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
            # TODO: think more about what size the queues should be. we don't want to overload postgres or s3
            table_executor = stack.enter_context(
                ThreadPoolExecutor(
                    max_workers=len(tables),
                    thread_name_prefix="Table",
                )
            )
            file_workers = max(2, (settings.s3_pool_size) // (len(tables) + 1))
            file_executors = {
                table_name: stack.enter_context(
                    ThreadPoolExecutor(
                        max_workers=file_workers,
                        thread_name_prefix=f"{table_name}File",
                    )
                )
                for table_name in tables
            }
            row_workers = max(2, (settings.postgres_pool_size) // (len(tables) + 1))
            row_group_executors = {
                table_name: stack.enter_context(
                    ThreadPoolExecutor(
                        max_workers=row_workers,
                        thread_name_prefix=f"{table_name}Rows",
                    )
                )
                for table_name in tables
            }

            LOGGER.info("workers", extra={"row": row_workers, "file": file_workers})

            futures = {
                table_executor.submit(
                    sync_parquet_to_db,
                    db_engine,
                    file_executors[table_name],
                    row_group_executors[table_name],
                    table_name,
                    progress_callbacks,
                    settings,
                ): table_name
                for table_name in tables
            }

            # TODO: start a thread for making sure imports are happening. if no imports for 10x the duration, force shutdown

            for f in as_completed(futures):
                table_name = futures[f]

                # will raise an exception if the future ended with one
                f.result()

                # all these futures should run forever
                # any completions are unexpected
                raise RuntimeError("table completed. this is unexpected", table_name)
        except KeyboardInterrupt:
            LOGGER.info("interrupted")
            # TODO: i don't love this. but it seems like we need it
            sys.exit(1)
        except Exception:
            LOGGER.exception("unhandled exception")
            # TODO: i don't love this. but it seems like we need it
            sys.exit(1)
        finally:
            LOGGER.info("shutting down")
            SHUTDOWN_EVENT.set()

            if table_executor is not None:
                table_executor.shutdown(wait=False, cancel_futures=True)

            if file_executor is not None:
                for file_executor in file_executors.values():
                    file_executor.shutdown(wait=False, cancel_futures=True)

            if row_group_executors is not None:
                for executor in row_group_executors.values():
                    executor.shutdown(wait=False, cancel_futures=True)

            # we did this during atexit, but that doesn't run until after the threads finish. and we want to force stop them now
            if db_engine is not None:
                db_engine.dispose()


if __name__ == "__main__":
    dotenv.load_dotenv(os.getenv("ENV_FILE", ".env"))

    settings = Settings()

    settings.initialize()

    if settings.interactive_debug:
        with launch_ipdb_on_exception():
            main(settings)
    else:
        main(settings)
