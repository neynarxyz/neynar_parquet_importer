import ast
import atexit
from datetime import UTC, datetime
import inspect
import logging
from pathlib import Path
from concurrent import futures
from datadog import statsd
import glob
import orjson
from os import path
from os import PathLike
import re
from time import time
import pyarrow.parquet as pq
from sqlalchemy import (
    MetaData,
    NullPool,
    QueuePool,
    Table,
    create_engine,
    select,
    text,
    update,
)
from sqlalchemy.dialects.postgresql.json import JSONB
from sqlalchemy.dialects.postgresql import insert as pg_insert
from tenacity import (
    after_log,
    before_sleep_log,
    retry,
    stop_after_attempt,
    wait_exponential_jitter,
)

from neynar_parquet_importer.row_filters import include_row

from .logger import LOGGER
from .s3 import parse_parquet_filename
from .settings import SHUTDOWN_EVENT, Settings, ShuttingDown

# TODO: detect this from the table
# TODO: this should be a dict of table names to column names
# arrays and json columns are stored as json in parquet because that was easier than dealing with the schema
JSON_COLUMNS = [
    "embeds",
    "mentions",
    "mentions_positions",
    "moderator_fids",
    "verified_addresses",
]


def sleep_or_raise_shutdown(t):
    if SHUTDOWN_EVENT.wait(t):
        raise ShuttingDown("shutting down instead of sleeping")


@retry(
    stop=stop_after_attempt(10), # TODO: make this a setting
    wait=wait_exponential_jitter(initial=0.4, max=10),
    sleep=sleep_or_raise_shutdown,
    # before=before_log(LOGGER, logging.DEBUG),
    after=after_log(LOGGER, logging.WARN),
    before_sleep=before_sleep_log(LOGGER, logging.WARN),
    reraise=True,
)
def init_db(uri, parquet_tables, settings: Settings):
    """Initialize the database with our simple schema."""
    # TODO: option to set `statement_timeout = 1000 * 30`  # 30 seconds

    if settings.postgres_poolclass == "NullPool":
        engine = create_engine(
            uri,
            connect_args={
                "connect_timeout": 30,
                # # TODO: this works on some servers, but others don't have permissions
                # "options": f"-c statement_timeout={statement_timeout}",
            },
            poolclass=NullPool,
        )

    else:
        engine = create_engine(
            uri,
            # TODO: make this a setting
            echo=False,
            connect_args={
                "connect_timeout": settings.postgres_connection_timeout,
                # # TODO: this works on some servers, but others don't have permissions
                # "options": f"-c statement_timeout={statement_timeout}",
            },
            poolclass=QueuePool,
            max_overflow=settings.postgres_max_overflow,
            pool_size=settings.postgres_pool_size,
            pool_timeout=60,
            pool_pre_ping=False,  # this slows things down too much
            pool_reset_on_return=True,
            pool_recycle=800,  # TODO: benchmark this. i see too many errors about connections being closed by the server
        )

    # TODO: this is not be forceful enough. we want to hard kill all the connections at exit
    atexit.register(engine.dispose)

    LOGGER.info("migrating...")

    pattern = r"schema/(?P<num>\d+)_(?P<sub_num>\d+)_(?P<parquet_db_name>[a-zA-Z0-9-]+)_(?P<parquet_schema_name>[a-zA-Z0-9-]+)_(?P<parquet_table_name>[a-zA-Z0-9_]+)\.sql"

    migrations = []

    parquet_tables_and_views = parquet_tables + settings.views.split(",")

    for filename in sorted(glob.glob("schema/*.sql")):
        m = re.match(pattern, filename)

        if m:
            parts = m.groupdict()

            LOGGER.debug(parts)

            if parts["parquet_db_name"] == "all":
                pass
            elif (
                parts["parquet_db_name"] == settings.parquet_s3_database
                and parts["parquet_schema_name"] == settings.parquet_s3_schema
                and parts["parquet_table_name"] in parquet_tables_and_views
            ):
                pass
            else:
                LOGGER.debug("Skipping %s", filename)
                continue
        else:
            LOGGER.debug("Skipping %s", filename)
            continue

        LOGGER.info("Applying %s", filename)

        with open(filename, "r") as f:
            migration = text(
                f.read().replace("${POSTGRES_SCHEMA}", settings.postgres_schema)
            )

            migrations.append(migration)

    if not migrations:
        raise RuntimeError("No migrations found")

    LOGGER.info("Connecting to the database for migrations")

    with engine.connect().execution_options(isolation_level="AUTOCOMMIT") as conn:
        for i, migration in enumerate(migrations):
            if LOGGER.isEnabledFor(logging.DEBUG):
                LOGGER.debug(
                    "applying migration", extra={"i": i, "migration": str(migration)}
                )
            else:
                LOGGER.info("applying migration", extra={"i": i})
            conn.execute(migration)

    LOGGER.info("migrations complete.")

    return engine


def check_for_past_incremental_import(
    engine,
    parquet_import_tracking: Table,
    settings: Settings,
    table: Table,
    backfill: bool,
) -> Path | None:
    """
    Returns the filename for the newest completed incremental.
    There may be some partially imported files after this.
    """

    stmt = (
        select(
            parquet_import_tracking.c.file_name,
        )
        .where(parquet_import_tracking.c.file_type == "incremental")
        .where(parquet_import_tracking.c.table_name == table.name)
        .where(parquet_import_tracking.c.file_version == settings.npe_version)
        .where(
            parquet_import_tracking.c.file_duration_s == settings.incremental_duration
        )
        .where(parquet_import_tracking.c.completed.is_(True))
        .where(parquet_import_tracking.c.backfill.is_(backfill))
        .order_by(parquet_import_tracking.c.end_timestamp.desc())
        .limit(1)
    )

    result = fetchone_with_retry(engine, stmt)

    if result is None:
        LOGGER.info(
            "no incremental files found",
            extra={
                "table": table.name,
                "file_version": settings.npe_version,
                "file_duration_s": settings.incremental_duration,
            },
        )
        return None

    latest_filename = Path(result[0])

    return latest_filename


def check_for_past_full_import(
    engine,
    parquet_import_tracking: Table,
    settings: Settings,
    table: Table,
    backfill: bool,
):
    """Returns the filename of the newest full import (there should really only be one). This may only be partially imported."""

    stmt = (
        select(
            parquet_import_tracking.c.file_name,
            parquet_import_tracking.c.completed,
            parquet_import_tracking.c.last_row_group_imported,
            parquet_import_tracking.c.total_row_groups,
        )
        .where(parquet_import_tracking.c.file_type == "full")
        .where(parquet_import_tracking.c.table_name == table.name)
        .where(parquet_import_tracking.c.file_version == settings.npe_version)
        .where(
            parquet_import_tracking.c.file_duration_s == settings.incremental_duration
        )
        .where(parquet_import_tracking.c.backfill.is_(backfill))
        .order_by(parquet_import_tracking.c.end_timestamp.desc())
        .limit(1)
    )

    result = fetchone_with_retry(engine, stmt)

    if result is None:
        return None

    latest_filename = Path(result[0])
    completed: bool = result[1]
    last_row_group_imported: int = result[2]
    total_row_groups: int = result[3]

    actually_completed = bool(
        last_row_group_imported is not None
        and last_row_group_imported == total_row_groups - 1
        # and completed
    )

    # if not completed but all the rows are done, we should mark it as completed now since it actually is done!
    if not completed and actually_completed:
        mark_completed(engine, parquet_import_tracking, [latest_filename])

    return (latest_filename, actually_completed)


def clean_jsonb_data(col_name, value):
    if value is None:
        return None

    # the parquet has the json as a string
    # i thought v3 tables were using the json extension, but that doesn't work how i thought
    try:
        if isinstance(value, str):
            if value.startswith("[{'") or value.startswith("{'"):
                return ast.literal_eval(value)
            else:
                return orjson.loads(value)
        elif isinstance(value, bytes):
            if value.startswith(b"[{'") or value.startswith(b"{'"):
                return ast.literal_eval(str(value))
            else:
                return orjson.loads(value)
    except Exception as exc:
        raise ValueError("failed to parse jsonb column", col_name, value, exc)

    # TODO: if this is a datetime column, it is from parquet in milliseconds, not seconds!
    return value


def get_tables(
    db_schema,
    engine,
    included_tables,
):
    """
    Fetches the tables (but not views) using SQLAlchemy.

    :param engine: SQLAlchemy engine connected to the database.
    :return: List of Table objects.
    """

    if db_schema == "public" or not db_schema:
        # i don't love this, but it keeps the table names consistent with the old code
        db_schema = None

    meta = MetaData(schema=db_schema)
    meta.reflect(bind=engine, views=False)

    if included_tables:
        filtered_table_names = included_tables + ["parquet_import_tracking"]

        filtered_tables = [
            table for table in meta.sorted_tables if table.name in filtered_table_names
        ]
    else:
        filtered_tables = meta.sorted_tables

    return {table.name: table for table in filtered_tables}


def import_parquet(
    engine,
    table: Table,
    local_file: Path,
    file_type,
    progress_callback,
    empty_callback,
    parquet_import_tracking: Table,
    row_group_executor,
    row_filters,
    settings: Settings,
    f_shutdown: futures.Future,
    backfill_start_timestamp: datetime | None,
    backfill_end_timestamp: datetime | None,
    backfill: bool = False,
):
    if isinstance(local_file, str):
        local_file = Path(local_file)

    parsed_filename = parse_parquet_filename(local_file)

    assert table.name == parsed_filename["table_name"]
    schema_name = parsed_filename["schema_name"]

    dd_tags = [
        f"parquet_table:{schema_name}.{table.name}",
        f"path:parquet-importer/{schema_name}.{table.name}",
    ]

    is_empty = local_file.suffix == ".empty"

    if is_empty:
        num_row_groups = 0
        # TODO: maybe have an option to return here instead of saving the ".empty" into the database
    else:
        try:
            parquet_file = pq.ParquetFile(local_file)
        except Exception as e:
            raise ValueError("Failed to read parquet file", local_file, e)

        num_row_groups = parquet_file.num_row_groups

    # Do NOT put 0 here. That would mean that we already imported row group 0!
    last_row_group_imported = None

    # TODO: rename imported_at to end_timestamp
    end_timestamp_dt = datetime.fromtimestamp(parsed_filename["end_timestamp"], UTC)

    # Prepare the insert statement
    stmt = pg_insert(parquet_import_tracking).values(
        table_name=table.name,
        file_name=str(local_file),
        file_type=file_type,
        file_version=settings.npe_version,
        file_duration_s=settings.incremental_duration,
        end_timestamp=end_timestamp_dt,
        is_empty=is_empty,
        last_row_group_imported=last_row_group_imported,
        total_row_groups=num_row_groups,
        backfill=backfill,
    )

    upsert_stmt = stmt.on_conflict_do_update(
        index_elements=["file_name"],  # Use the unique constraint columns
        set_={
            # No actual data changes; this is a no-op update
            "last_row_group_imported": parquet_import_tracking.c.last_row_group_imported,
        },
    ).returning(
        parquet_import_tracking.c.id, parquet_import_tracking.c.last_row_group_imported
    )

    row = fetchone_with_retry(engine, upsert_stmt)

    # Extract the id and last_row_group_imported
    tracking_id = row.id
    last_row_group_imported = row.last_row_group_imported

    if is_empty:
        # LOGGER.debug(
        #     "imported empty file", extra={"id": tracking_id, "file": local_filename}
        # )

        # no need to continue on here. we can commit and return early
        empty_callback(1)

        file_age_s = time() - parsed_filename["end_timestamp"]

        statsd.gauge("parquet_file_age_s", file_age_s, tags=dd_tags)

        # there is no row age for an empty file. use the file age instead
        statsd.gauge("parquet_row_age_s", file_age_s, tags=dd_tags)
        return

    if last_row_group_imported is None:
        start_row_group = 0
    else:
        start_row_group = last_row_group_imported + 1

    new_steps = num_row_groups - start_row_group

    if new_steps == 0:
        LOGGER.info("%s has already been imported", local_file)
        return

    if last_row_group_imported is not None:
        LOGGER.info(
            "%s has resumed importing",
            local_file,
            extra={
                "start_row_group": start_row_group,
                "new_steps": new_steps,
            },
        )

    # LOGGER.debug(
    #     "%s more steps from %s",
    #     f"{new_steps:_}",
    #     table_name,
    # )

    # update the progress counter with our new step total
    progress_callback.more_steps(new_steps)

    primary_key_columns = table.primary_key.columns.values()

    if settings.datadog_enabled:
        cu_metric = settings.cu_mode.metric()
    else:
        cu_metric = None

    if cu_metric:
        neynar_api_client = settings.neynar_api_client()

        pricing_key = f"{schema_name}.{table.name}"

        cu_prices = neynar_api_client.get_portal_pricing("indexer_service")

        row_cu_cost = cu_prices.get(pricing_key)
        filtered_row_cu_cost = 0

        if row_cu_cost is None:
            logging.warning("unknown cu cost", extra={"pricing_key": pricing_key})
            row_cu_cost = 0

        if row_filters:
            filtered_row_cu_cost = row_cu_cost * settings.filtered_row_multiplier
            # disable row_cu_cost since filtered cost applies to all rows
            row_cu_cost = 0

        # # TODO: this is too verbose
        # logging.info(
        #     "pricing settings",
        #     extra={
        #         "cu_mode": settings.cu_mode,
        #         "cu_metric": cu_metric,
        #         "row_cu_cost": row_cu_cost,
        #         "filtered_row_cu_cost": filtered_row_cu_cost,
        #         "pricing_key": pricing_key,
        #     },
        # )
    else:
        row_cu_cost = 0
        filtered_row_cu_cost = 0

    # Read the data in batches
    # the batches are imported in parallel. the tracking table is updated in submit order
    fs = []
    for i in range(start_row_group, num_row_groups):
        # TODO: larger batches with `pf.iter_batches(batch_size=X)` instead of row groups

        # TODO: debugging option to only import a few row groups

        # LOGGER.debug(
        #     "Queueing upsert #%s/%s for %s",
        #     f"{i+1:_}",
        #     f"{num_row_groups:_}",
        #     table_name,
        # )

        f = row_group_executor.submit(
            process_batch,
            dd_tags,
            engine,
            i,
            settings.npe_version,
            parquet_file,
            parsed_filename,
            primary_key_columns,
            progress_callback,
            row_filters,
            table,
            cu_metric,
            row_cu_cost,
            filtered_row_cu_cost,
            backfill_start_timestamp,
            backfill_end_timestamp,
        )

        fs.append(f)

        # TODO: if fs is really long, wait? or maybe do update_tracking_stmt here, too?

    # LOGGER.debug("waiting for %s futures", len(fs))

    log_every_n = max(10, (num_row_groups // 100) or (num_row_groups // 10))

    # update our database entry's last_row_group_imported
    # read them in order rather than with as_completed
    # TODO: have all the tables do this in another thread?
    update_tracking_stmt = parquet_import_tracking.update().where(
        parquet_import_tracking.c.id == tracking_id
    )
    i = file_age_s = row_age_s = None
    while fs:
        f = fs.pop(0)

        done, not_done = futures.wait(
            [f, f_shutdown], return_when=futures.FIRST_COMPLETED
        )

        if f_shutdown in done:
            raise ShuttingDown("shutting down during import_parquet")

        assert f in done

        # no need to call update for every entry if a bunch are done. skip to the last finished one
        if fs:
            while fs:
                if fs[0].done():
                    f = fs.pop(0)
                else:
                    break

        # no need for a timeout here because it is marked done
        (i, file_age_s, row_age_s, last_updated_at) = f.result()

        # logging.debug(
        #     "completed",
        #     extra={
        #         "i": i,
        #         "file_age_s": file_age_s,
        #         "row_age_s": row_age_s,
        #         "last_updated_at": last_updated_at.timestamp(),
        #     },
        # )

        execute_with_retry(
            engine, update_tracking_stmt.values(last_row_group_imported=i)
        )

        # TODO: metric here?
        if num_row_groups > 1 and i < num_row_groups - 1 and i % log_every_n == 0:
            LOGGER.info(
                "Completed upsert #%s/%s for %s",
                f"{i + 1:_}",
                f"{num_row_groups:_}",
                table.name,
                extra={
                    "file_age_s": file_age_s,
                    "row_age_s": row_age_s,
                    "last_updated_at": last_updated_at.timestamp(),
                },
            )

    file_size = path.getsize(local_file)

    # TODO: i'd like to emit this metric in the process_batch function, but I'm not sure how to get the size of the batch
    statsd.increment(
        "parquet_bytes_imported",
        value=file_size,
        tags=dd_tags,
    )

    # TODO: datadog metrics here?
    if i is not None and num_row_groups == i + 1:
        LOGGER.info(
            "finished import",
            extra={
                "file_age_s": file_age_s,
                "row_age_s": row_age_s,
                "table": table.name,
                "file_name": str(local_file),
                "num_row_groups": num_row_groups,
                "num_rows": parquet_file.metadata.num_rows,
                "file_size": file_size,
            },
        )
    else:
        LOGGER.info(
            "incomplete import",
            extra={
                "file_age_s": file_age_s,
                "row_age_s": row_age_s,
                "table": table.name,
                "file_name": str(local_file),
                "i": i,
                "num_row_groups": num_row_groups,
                "num_rows": parquet_file.metadata.num_rows,
                "file_size": file_size,
            },
        )


def mark_completed(db_engine, parquet_import_tracking, completed_filenames):
    if not completed_filenames:
        return

    completed_filenames = [str(c) for c in completed_filenames]

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



def our_after_log(retry_state):
    """
    Tenacity “after” hook that reports where the wrapped function
    was invoked from (file:line) on every attempt.
    """
    # 0 = this frame, 1 = internal tenacity wrapper, 2 = original caller
    caller = inspect.stack()[2]
    LOGGER.warning(
        "attempt %d on %s raised %s  ← called from %s:%d",
        retry_state.attempt_number,
        retry_state.fn.__name__,
        retry_state.outcome.exception() if retry_state.outcome else None,
        caller.filename,
        caller.lineno,
    )
    # Avoid reference cycles created by inspect
    del caller


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=0.4, max=10),
    sleep=sleep_or_raise_shutdown,
    # before=before_log(LOGGER, logging.DEBUG),
    after=after_log(LOGGER, logging.WARN),
    before_sleep=before_sleep_log(LOGGER, logging.WARN),
    reraise=True,
)
def execute_with_retry(engine, stmt):
    with engine.connect() as conn:
        try:
            result = conn.execute(stmt)
        except Exception as e:
            if hasattr(e, "statement"):
                e.statement = None
            raise

        conn.commit()
        return result


@retry(
    stop=stop_after_attempt(5),
    wait=wait_exponential_jitter(initial=0.4, max=10),
    sleep=sleep_or_raise_shutdown,
    # before=before_log(LOGGER, logging.DEBUG),
    after=after_log(LOGGER, logging.WARN),
    before_sleep=before_sleep_log(LOGGER, logging.WARN),
    reraise=True,
)
def fetchone_with_retry(engine, stmt):
    with engine.connect() as conn:
        result = conn.execute(stmt)
        row = result.fetchone()
        conn.commit()
        return row


def maximum_parquet_age(full_filename: None | str):
    """Only 3 weeks of files are kept in s3"""
    if full_filename is PathLike:
        parsed_filename = parse_parquet_filename(full_filename)
        return parsed_filename["end_timestamp"]

    return time() - 60 * 60 * 24 * 7 * 3


# NOTE: You can modify the data however you want here. Do things like pull values out of json columns or skip columns entirely.
# TODO: have a helper function here that makes it easier to clean up the data
def process_batch(
    dd_tags,
    engine,
    i,
    npe_version,
    parquet_file,
    parsed_filename,
    primary_key_columns,
    progress_callback,
    row_filters,
    table,
    cu_metric: str | None,
    row_cu_cost: int,
    filtered_row_cu_cost: int,
    backfill_start_timestamp: int | None,
    backfill_end_timestamp: int | None,
):
    # Get global settings to determine processing path
    from .context import get_global_settings
    settings = get_global_settings()
    
    LOGGER.info(f"🔀 process_batch: settings = {settings.database_backend if settings else 'None'}")
    
    # ZERO-COST PATH: If PostgreSQL or no settings, use existing logic directly
    if settings is None or settings.database_backend == "postgresql":
        LOGGER.info("📊 Taking PostgreSQL path (original processing)")
        return _process_batch_original(
            dd_tags, engine, i, npe_version, parquet_file, 
            parsed_filename, primary_key_columns, progress_callback,
            row_filters, table, cu_metric, row_cu_cost, 
            filtered_row_cu_cost, backfill_start_timestamp, 
            backfill_end_timestamp
        )
    
    # NEW PATH: Only for non-PostgreSQL backends
    else:
        LOGGER.info(f"🚀 Taking {settings.database_backend} path (transformation processing)")
        return _process_batch_with_transformation(
            dd_tags, engine, i, npe_version, parquet_file, 
            parsed_filename, primary_key_columns, progress_callback,
            row_filters, table, cu_metric, row_cu_cost, 
            filtered_row_cu_cost, backfill_start_timestamp, 
            backfill_end_timestamp, settings
        )


def _process_batch_original(
    dd_tags,
    engine,
    i,
    npe_version,
    parquet_file,
    parsed_filename,
    primary_key_columns,
    progress_callback,
    row_filters,
    table,
    cu_metric: str | None,
    row_cu_cost: int,
    filtered_row_cu_cost: int,
    backfill_start_timestamp: int | None,
    backfill_end_timestamp: int | None,
):
    """Existing process_batch logic moved here unchanged"""
    # This is too verbose
    # LOGGER.debug("starting batch #%s", i)

    # TODO: is a row group really the right size here?
    # TODO: postgres has a maximum item count of 65535! need to make sure split up the sql if its too big
    batch = parquet_file.read_row_group(i)

    # TODO: detect tables that need deduping automatically. i think its any that have multiple primary key col
    if table.name in ["profile_with_addresses"]:
        # TODO: check that we are in the right schema too. this is only needed for farcaster.profile_with_addresses
        # this view needs de-duping
        data = batch.to_pydict()

        # collect into a different dict so that we can remove dupes
        rows = {
            tuple(data[pk_col.name][i] for pk_col in primary_key_columns): {
                col_name: data[col_name][i] for col_name in data
            }
            for i in range(len(batch))
        }
        # discard the keys
        rows = list(rows.values())

        # if len(batch) > len(rows):
        #     LOGGER.debug(
        #         "Dropped %s rows with duplicate primary keys",
        #         len(batch) - len(rows),
        #     )
    else:
        # Direct conversion to Python-native types for sqlalchemy
        # TODO: this is probably making this way slower than necessary. im sure there are libraries to do this faster. df -> postgres
        rows = batch.to_pylist()

    # make sure we aren't passing timestamps in for direct_import or main call-ins
    if row_filters or backfill_start_timestamp is not None or backfill_end_timestamp is not None:
        orig_rows_len = len(rows)

        # TODO: check versions of the filters. we might want to support graphql or other formats in the near future
        rows = list(filter(lambda row: include_row(row, row_filters, backfill_start_timestamp, backfill_end_timestamp), rows))

        rows_len = len(rows)

        filtered_rows = orig_rows_len - rows_len

        extra = {
            "num_filtered": filtered_rows,
            "rows": rows_len,
            "table": table.name,
        }

        # where we calculate cost for filtered rows
        # here calculate the total cost of all the rows, whether filtered or not
        if cu_metric:
            cu_cost = orig_rows_len * filtered_row_cu_cost

            extra["cu_cost"] = cu_cost

            # TODO: add another tag that shows that this is for filtered data?
            statsd.increment(
                cu_metric,
                value=cu_cost,
                tags=dd_tags,
            )

        LOGGER.debug("filtered", extra=extra)

        statsd.increment(
            "num_parquet_rows_filtered",
            value=filtered_rows,
            tags=dd_tags,
        )

    else:
        rows_len = len(rows)
        filtered_rows = 0

    if rows:
        row_keys = rows[0].keys()

        # TODO: i don't love this. parquet apply things will be much faster. but we already turned it into a python object. will require a larger refactor
        # loop col_names first and only call clean on ones that need changes
        for col_name in row_keys:
            col = table.c[col_name]

            if isinstance(col.type, JSONB):
                for row in rows:
                    row[col_name] = clean_jsonb_data(col_name, row[col_name])

        # TODO: use Abstract Base Classes to make this easy to extend/transform

        # insert or update the rows
        stmt = pg_insert(table).values(rows)

        # only upsert where updated_at is newer than the existing row
        upsert_stmt = stmt.on_conflict_do_update(
            index_elements=primary_key_columns,
            set_={col: stmt.excluded[col] for col in row_keys},
            where=(stmt.excluded["updated_at"] >= table.c.updated_at),
        )

        execute_with_retry(engine, upsert_stmt)

    now = time()

    file_age_s = now - parsed_filename["end_timestamp"]

    if rows:
        last_updated_at = rows[-1]["updated_at"]
    else:
        last_updated_at = datetime.fromtimestamp(parsed_filename["end_timestamp"], UTC)

    row_age_s = now - last_updated_at.timestamp()

    if file_age_s > row_age_s:
        # this happened in the past when timezones were not handled correctly
        LOGGER.warning(
            "bad row age!",
            extra={
                "now": now,
                "file_age_s": file_age_s,
                "row_age_s": row_age_s,
                "last_updated_at": last_updated_at.timestamp(),
            },
        )

    statsd.gauge("parquet_file_age_s", file_age_s, tags=dd_tags)
    statsd.gauge("parquet_row_age_s", row_age_s, tags=dd_tags)
    statsd.increment(
        "num_parquet_rows_imported",
        value=rows_len,
        tags=dd_tags,
    )

    # we only calculate the cost here if row filtering is not applied
    if cu_metric and row_cu_cost > 0:
        cu_cost = rows_len * row_cu_cost

        logging.debug("cu_cost", extra={"cu_cost": cu_cost, "num_rows": rows_len})

        statsd.increment(
            cu_metric,
            value=cu_cost,
            tags=dd_tags,
        )

    progress_callback(1)

    # TODO: better return type for this so we don't mix up values
    # TODO: include the cu cost and filtered rows in this?
    return (i, file_age_s, row_age_s, last_updated_at)


def _process_batch_with_transformation(
    dd_tags,
    engine,
    i,
    npe_version,
    parquet_file,
    parsed_filename,
    primary_key_columns,
    progress_callback,
    row_filters,
    table,
    cu_metric: str | None,
    row_cu_cost: int,
    filtered_row_cu_cost: int,
    backfill_start_timestamp: int | None,
    backfill_end_timestamp: int | None,
    settings: Settings,
):
    """New transformation-based processing for graph databases"""
    from .database.factory import DatabaseFactory
    from time import time
    from datetime import UTC, datetime
    
    LOGGER.info(f"🔄 Processing batch {i} with transformation (table: {table.name})")
    
    # Get backend and transformer
    LOGGER.info("🏗️  Creating backend and transformer...")
    backend = DatabaseFactory.create_backend(settings)
    transformer = DatabaseFactory.create_transformer(settings)
    LOGGER.info(f"   Backend: {type(backend).__name__}")
    LOGGER.info(f"   Transformer: {type(transformer).__name__}")
    
    # Initialize the backend with connection parameters
    LOGGER.info("🔌 Initializing backend connection...")
    backend.init_db(None, [table.name], settings)  # Neo4j doesn't use the URI parameter, it uses settings
    LOGGER.info("   Backend initialized successfully")
    
    # Convert PyArrow batch to rows (existing logic)
    LOGGER.info(f"📖 Reading row group {i}...")
    batch = parquet_file.read_row_group(i)
    rows = batch.to_pylist()
    LOGGER.info(f"   Read {len(rows)} rows from parquet")
    
    # Apply row filters (existing logic)
    if row_filters or backfill_start_timestamp is not None or backfill_end_timestamp is not None:
        orig_rows_len = len(rows)
        rows = list(filter(lambda row: include_row(row, row_filters, backfill_start_timestamp, backfill_end_timestamp), rows))
        rows_len = len(rows)
        filtered_rows = orig_rows_len - rows_len
        
        # Log filtered rows
        extra = {
            "num_filtered": filtered_rows,
            "rows": rows_len,
            "table": table.name,
        }
        
        if cu_metric:
            cu_cost = orig_rows_len * filtered_row_cu_cost
            extra["cu_cost"] = cu_cost
            statsd.increment(cu_metric, value=cu_cost, tags=dd_tags)
        
        LOGGER.debug("filtered", extra=extra)
        statsd.increment("num_parquet_rows_filtered", value=filtered_rows, tags=dd_tags)
    else:
        rows_len = len(rows)
    
    LOGGER.info(f"📊 After filtering: {rows_len} rows to process")
    
    # Transform rows to operations
    LOGGER.info("🔄 Transforming rows to operations...")
    operations = transformer.transform_table(table.name, rows)
    LOGGER.info(f"   Created {len(operations)} operations")
    
    # Execute via backend
    LOGGER.info("💾 Executing operations via backend...")
    backend.import_operations(operations)
    LOGGER.info("   Operations executed successfully")
    
    # Calculate metrics (similar to original)
    now = time()
    file_age_s = now - parsed_filename["end_timestamp"]
    
    if rows:
        # Try to get updated_at from the last row, fallback to timestamp
        last_row = rows[-1]
        if "updated_at" in last_row and last_row["updated_at"]:
            last_updated_at = last_row["updated_at"]
        else:
            last_updated_at = datetime.fromtimestamp(parsed_filename["end_timestamp"], UTC)
    else:
        last_updated_at = datetime.fromtimestamp(parsed_filename["end_timestamp"], UTC)
    
    row_age_s = now - last_updated_at.timestamp()
    
    # Record metrics
    statsd.gauge("parquet_file_age_s", file_age_s, tags=dd_tags)
    statsd.gauge("parquet_row_age_s", row_age_s, tags=dd_tags)
    statsd.increment("num_parquet_rows_imported", value=rows_len, tags=dd_tags)
    
    if cu_metric and row_cu_cost > 0:
        cu_cost = rows_len * row_cu_cost
        statsd.increment(cu_metric, value=cu_cost, tags=dd_tags)
    
    progress_callback(1)
    
    return (i, file_age_s, row_age_s, last_updated_at)
