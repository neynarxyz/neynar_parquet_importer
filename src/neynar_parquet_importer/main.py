import logging
import os
import signal
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from contextlib import ExitStack
import traceback
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
    check_for_past_full_import,
    check_for_past_incremental_import,
    execute_with_retry,
    get_tables,
    import_parquet,
    init_db,
    maximum_parquet_age,
)
from .s3 import (
    download_incremental,
    download_latest_full,
    get_s3_client,
    parse_parquet_filename,
)
from .settings import SHUTDOWN_EVENT, Settings, ShuttingDown

LOGGER = logging.getLogger("app")


# TODO: fetch this from env? tools to generate this list from the s3 bucket?
# NOTE: "messages" is very large and so is not part of parquet exports
ALL_TABLES = {
    # for these, set `npe_version=v2` `parquet_s3_schema=farcaster` `incremental_duration=300`
    ("public-postgres", "farcaster"): [
        "account_verifications",
        "blocks",
        "casts",  # NOTE: `casts` is VERY large with LOTS of writes!
        "channel_follows",
        "channel_members",
        "channels",
        "fids",
        "fnames",
        # "links",  # NOTE: please use the nindexer follows table instead
        "power_users",
        # "profile_with_addresses",  # NOTE: please use the nindexer profiles_with_verifications VIEW instead
        "reactions",  # NOTE: `reactions` is VERY large with LOTS of writes!
        "signers",
        "storage",
        "user_data",
        "user_labels",
        # "verifications",  # NOTE: please use the nindexer verifications table instead
        "warpcast_power_users",
    ],
    # for these, set `npe_version=v3` `parquet_s3_schema=nindexer` `incremental_duration=1`
    ("public-postgres", "nindexer"): [
        # "follow_counts",  # NOTE: this table costs extra. Talk to us if you need it
        "follows",
        # "neynar_user_scores",  # NOTE: this table costs extra. Talk to us if you need it
        "profiles",
        "verifications",
    ],
}

ALL_VIEWS = {
    # for these, set `npe_version=v2` `parquet_s3_schema=farcaster` `incremental_duration=300`
    ("public-postgres", "farcaster"): [],
    # for these, set `npe_version=v3` `parquet_s3_schema=nindexer` `incremental_duration=1`
    ("public-postgres", "nindexer"): [
        # this view require nindexer's "profiles" and "verifications" tables
        "profiles_with_verifications",
    ],
}


def sync_parquet_to_db(
    db_engine,
    download_threadpool: ThreadPoolExecutor,
    file_executor,
    row_group_executor,
    table,
    parquet_import_tracking,
    progress_callbacks,
    settings: Settings,
):
    """Function that runs forever (barring exceptions) to download and import parquet files for a table.

    TODO: run downloads and imports in parallel to improve initial sync times
    """
    completed_filenames = []
    try:
        last_import_filename = None

        s3_client = get_s3_client(settings)

        existing_full_result = check_for_past_full_import(
            db_engine,
            parquet_import_tracking,
            settings,
            table,
        )

        incremental_filename = check_for_past_incremental_import(
            db_engine,
            parquet_import_tracking,
            settings,
            table,
        )

        if incremental_filename:
            parsed_filename = parse_parquet_filename(incremental_filename)

            if parsed_filename["end_timestamp"] <= maximum_parquet_age():
                LOGGER.warning(
                    "incremental is too old. starting over",
                    extra={"table": table.name},
                )
                existing_full_result = None
                full_filename = None
                full_completed = False
                last_import_filename = None
                incremental_filename = None

        if existing_full_result is not None:
            (full_filename, full_completed) = existing_full_result
            LOGGER.debug(
                "full found",
                extra={
                    "table": table.name,
                    "full_filename": full_filename,
                    "completed": full_completed,
                },
            )

            last_import_filename = full_filename
        else:
            LOGGER.debug("no full in the tracking table", extra={"table": table.name})
            full_filename = None
            full_completed = False
            last_import_filename = None
            incremental_filename = None

        if full_completed:
            # if we have a completed full, we probably have incrementals
            incremental_filename = check_for_past_incremental_import(
                db_engine,
                parquet_import_tracking,
                settings,
                table,
            )

            if incremental_filename:
                # if we have imported an incremental. we should start there instead of at the full
                if not os.path.exists(incremental_filename):
                    last_start_timestamp = parse_parquet_filename(incremental_filename)[
                        "start_timestamp"
                    ]

                    incremental_filename = download_incremental(
                        download_threadpool,
                        s3_client,
                        settings,
                        table,
                        last_start_timestamp,
                        progress_callbacks["incremental_bytes"],
                        progress_callbacks["empty_steps"],
                    )

                if incremental_filename:
                    import_parquet(
                        db_engine,
                        table,
                        incremental_filename,
                        "incremental",
                        progress_callbacks["incremental_steps"],
                        progress_callbacks["empty_steps"],
                        parquet_import_tracking,
                        row_group_executor,
                        settings,
                    )

                    last_import_filename = incremental_filename

                    mark_completed(
                        db_engine, parquet_import_tracking, [incremental_filename]
                    )
                else:
                    raise ValueError("incremental_filename is missing")
        else:
            if full_filename is not None:
                parsed_filename = parse_parquet_filename(full_filename)

                if parsed_filename["end_timestamp"] <= maximum_parquet_age():
                    LOGGER.warning(
                        "full is too old. starting over", extra={"table": table.name}
                    )
                    full_filename = None

            # the full is not completed (or not even started). start there
            # TODO: spawn this so we can check for incrementals while this is downloading
            if full_filename is None:
                # if no full export, download the latest one
                full_filename = download_latest_full(
                    download_threadpool,
                    s3_client,
                    settings,
                    table,
                    progress_callbacks["full_bytes"],
                )

            import_parquet(
                db_engine,
                table,
                full_filename,
                "full",
                progress_callbacks["full_steps"],
                progress_callbacks["empty_steps"],
                parquet_import_tracking,
                row_group_executor,
                settings,
            )

            mark_completed(db_engine, parquet_import_tracking, [full_filename])
            full_completed = True
            last_import_filename = full_filename

        next_start_timestamp = parse_parquet_filename(last_import_filename)[
            "end_timestamp"
        ]
        next_end_timestamp = next_start_timestamp + settings.incremental_duration

        max_wait_duration = max(90, 4 * settings.incremental_duration)

        # download all the incrementals. loops forever
        fs = []
        while not SHUTDOWN_EVENT.is_set():
            # mark files completed in order. this keeps us from skipping items if we have to restart
            while fs:
                if fs[0].done():
                    f = fs.pop(0)

                    incremental_filename = f.result()

                    if incremental_filename is None:
                        raise ShuttingDown("incremental_filename is None")

                    completed_filenames.append(incremental_filename)

                    # LOGGER.debug(
                    #     "queued completion",
                    #     extra={"f": f, "n": len(completed_filenames)},
                    # )
                elif fs[0].cancelled():
                    LOGGER.debug("cancelled")
                    return
                else:
                    if time.time() >= next_start_timestamp:
                        # time to spawn the next file
                        # LOGGER.debug(
                        #     "future not complete in time",
                        #     extra={"f": fs[0]},
                        # )
                        break
                    # else:
                    #     LOGGER.debug(
                    #         "waiting for future to complete",
                    #         extra={"f": fs[0]},
                    #     )

            mark_completed(db_engine, parquet_import_tracking, completed_filenames)
            completed_filenames.clear()

            sleep_amount = max(0, next_start_timestamp - time.time())

            if fs:
                # sleep a maximum of one second since we should loop to see if tasks are done
                sleep_amount = min(1, sleep_amount)

            if SHUTDOWN_EVENT.wait(sleep_amount):
                LOGGER.debug(
                    "shutting down sync_parquet_to_db",
                    extra={"table": table.name},
                )
                return

            # spawn a task on file_executor here
            f = file_executor.submit(
                download_and_import_incremental_parquet,
                db_engine,
                download_threadpool,
                s3_client,
                table,
                max_wait_duration,
                next_start_timestamp,
                progress_callbacks,
                parquet_import_tracking,
                row_group_executor,
                settings,
            )
            fs.append(f)

            next_start_timestamp += settings.incremental_duration
            next_end_timestamp += settings.incremental_duration
    except ShuttingDown:
        return
    except Exception as e:
        if e.args == ("cannot schedule new futures after shutdown",):
            LOGGER.debug(
                "Executor is shutting down during sync_parquet_to_db",
                extra={"table": table.name},
            )
            return

        LOGGER.exception(
            "exception inside sync_parquet_to_db",
            extra={"table": table.name},
        )
        SHUTDOWN_EVENT.set()
        raise
    finally:
        # don't lose any progress
        if completed_filenames:
            logging.info(
                "final mark_completed",
                extra={
                    "table": table.name,
                    "last_file": completed_filenames[-1],
                    "num_files": len(completed_filenames),
                },
            )
            mark_completed(db_engine, parquet_import_tracking, completed_filenames)
            completed_filenames.clear()

        # this should run forever. any exit here means we should shut down the whole app
        SHUTDOWN_EVENT.set()


def mark_completed(db_engine, parquet_import_tracking, completed_filenames):
    if not completed_filenames:
        return

    stmt = (
        update(parquet_import_tracking)
        .where(parquet_import_tracking.c.file_name.in_(completed_filenames))
        .values(completed=True)
    )

    # # this is too verbose
    LOGGER.debug(
        "completed",
        extra={
            "last_file": completed_filenames[-1],
            "num_files": len(completed_filenames),
        },
    )

    return execute_with_retry(db_engine, stmt)


def download_and_import_incremental_parquet(
    db_engine,
    download_threadpool: ThreadPoolExecutor,
    s3_client,
    table: Table,
    max_wait_duration,
    next_start_timestamp,
    progress_callbacks,
    parquet_import_tracking,
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
                    "table": table.name,
                    "next_start_timestamp": next_start_timestamp,
                    "now": now,
                }
                if settings.exit_after_max_wait:
                    # this is a sledge hammer. think more about this!
                    raise ValueError(
                        "Max wait exceeded. No parquet files were imported recently",
                        extra,
                    )
                else:
                    LOGGER.warning(
                        "Max wait exceeded. No parquet files were imported recently",
                        extra=extra,
                    )

            incremental_filename = download_incremental(
                download_threadpool,
                s3_client,
                settings,
                table,
                next_start_timestamp,
                progress_callbacks["incremental_bytes"],
                progress_callbacks["empty_steps"],
            )

            if incremental_filename is None:
                # TODO: how long should we sleep? polling isn't great, but SNS seems inefficient with a bunch of tables and short durations
                sleep_amount = min(30, settings.incremental_duration / 2.0)

                extra = {
                    "table": table.name,
                    "sleep_amount": sleep_amount,
                    "start_timestamp": next_start_timestamp,
                }

                LOGGER.debug(
                    "This incremental should be ready soon. Sleeping",
                    extra=extra,
                )

                if SHUTDOWN_EVENT.wait(sleep_amount):
                    LOGGER.debug(
                        "shutting down during download_and_import_incremental_parquet",
                        extra=extra,
                    )
                    return

        import_parquet(
            db_engine,
            table,
            incremental_filename,
            "incremental",
            progress_callbacks["incremental_steps"],
            progress_callbacks["empty_steps"],
            parquet_import_tracking,
            row_group_executor,
            settings,
        )

        # we got a file. reset max wait
        if max_wait_duration:
            max_wait = time.time() + max_wait_duration
    except ShuttingDown:
        return
    except Exception as e:
        if e.args == ("cannot schedule new futures after shutdown",):
            LOGGER.debug("Executor is shutting down during sync_parquet_to_db")
            return

        LOGGER.exception("exception inside download_and_import_incremental_parquet")
        SHUTDOWN_EVENT.set()
        raise

    return incremental_filename


def queue_hard_shutdown():
    # TODO: use a threading.Timer and have a watchdog thread that checks for no progress
    for _ in range(10):
        if len(threading.enumerate()) == 2:
            LOGGER.info("no threads left. shutting down cleanly")
            return

        time.sleep(1)

    LOGGER.error("hard shutdown!")
    for thread in threading.enumerate():
        LOGGER.warning(
            f"Thread {thread.name} (ID: {thread.ident}):\n{traceback.format_stack()}"
        )

    os.kill(0, signal.SIGKILL)


def start_shutdown():
    LOGGER.info("starting to shut down")

    SHUTDOWN_EVENT.set()

    # TODO: if unix, use SIGALARM?
    if threading.current_thread() == threading.main_thread():
        threading.Thread(target=queue_hard_shutdown).start()


def main(settings: Settings):
    with ExitStack() as stack:
        db_engine = table_executor = file_executor = row_group_executors = None
        try:
            if settings.tables:
                table_names = settings.tables.split(",")
            else:
                table_names = ALL_TABLES[
                    (settings.parquet_s3_database, settings.parquet_s3_schema)
                ]

            LOGGER.info("Tables: %s", ",".join(table_names))

            db_engine = init_db(str(settings.postgres_dsn), table_names, settings)

            tables = get_tables(settings.postgres_schema, db_engine, table_names)

            # TODO: test the s3 client here?

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
                    max_workers=len(table_names),
                    thread_name_prefix="Table",
                )
            )
            file_executors = {
                table_name: stack.enter_context(
                    ThreadPoolExecutor(
                        max_workers=settings.file_workers,
                        thread_name_prefix=f"{table_name}File",
                    )
                )
                for table_name in table_names
            }
            download_executors = {
                table_name: stack.enter_context(
                    ThreadPoolExecutor(
                        max_workers=settings.download_workers,
                        thread_name_prefix=f"{table_name}Download",
                    )
                )
                for table_name in table_names
            }
            row_group_executors = {
                table_name: stack.enter_context(
                    ThreadPoolExecutor(
                        max_workers=settings.row_workers,
                        thread_name_prefix=f"{table_name}Rows",
                    )
                )
                for table_name in table_names
            }

            pool_size_needed = settings.row_workers * len(
                row_group_executors
            ) + settings.file_workers * len(file_executors)

            if pool_size_needed > settings.postgres_pool_size:
                LOGGER.error(
                    "postgres_pool_size is too small! high paralellism may crash the app!",
                    extra={
                        "db_available": settings.postgres_pool_size,
                        "db_needed": pool_size_needed,
                        "row": settings.row_workers,
                        "file": settings.file_workers,
                    },
                )
            else:
                LOGGER.info(
                    "workers",
                    extra={
                        "db_available": settings.postgres_pool_size,
                        "db_needed": pool_size_needed,
                        "row": settings.row_workers,
                        "file": settings.file_workers,
                    },
                )

            futures = {
                table_executor.submit(
                    sync_parquet_to_db,
                    db_engine,
                    download_executors[table_name],
                    file_executors[table_name],
                    row_group_executors[table_name],
                    tables[table_name],
                    tables["parquet_import_tracking"],
                    progress_callbacks,
                    settings,
                ): table_name
                for table_name in table_names
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
            LOGGER.exception("exiting because of exception")
            # TODO: i don't love this. but it seems like we need it
            sys.exit(1)
        finally:
            start_shutdown()

            if table_executor is not None:
                table_executor.shutdown(wait=False, cancel_futures=True)

            if file_executor is not None:
                for file_executor in file_executors.values():
                    file_executor.shutdown(wait=False, cancel_futures=True)

            if row_group_executors is not None:
                for executor in row_group_executors.values():
                    executor.shutdown(wait=False, cancel_futures=True)


if __name__ == "__main__":
    dotenv.load_dotenv(os.getenv("ENV_FILE", ".env"))

    settings = Settings()

    settings.initialize()

    if settings.interactive_debug:
        with launch_ipdb_on_exception():
            main(settings)
    else:
        main(settings)
