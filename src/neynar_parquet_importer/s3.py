from concurrent.futures import ThreadPoolExecutor
from functools import cache
import logging
import math
import os
import re
import shutil
import boto3
from botocore.config import Config
from sqlalchemy import Table

from neynar_parquet_importer.progress import ProgressCallback

from .logger import LOGGER
from .settings import Settings


def parse_parquet_filename(filename):
    basename = os.path.basename(filename)

    match = re.match(r"(.+)-(.+)-(\d+)-(\d+)\.(?:parquet|empty)", basename)
    if match:
        return {
            "schema_name": match.group(1),
            "table_name": match.group(2),
            "start_timestamp": int(match.group(3)),
            "end_timestamp": int(match.group(4)),
        }
    else:
        raise ValueError("Parquet filename does not match expected format.", filename)


def download_latest_full(
    download_threadpool: ThreadPoolExecutor,
    s3_client,
    settings: Settings,
    table: Table,
    progress_callback,
):
    s3_prefix = settings.parquet_s3_prefix() + "full/"

    full_export_prefix = s3_prefix + f"{settings.parquet_s3_schema}-{table.name}-0-"

    paginator = s3_client.get_paginator("list_objects_v2")
    operation_parameters = {
        "Bucket": settings.parquet_s3_bucket,
        "Prefix": full_export_prefix,
    }
    page_iterator = paginator.paginate(**operation_parameters)
    latest_file = None
    for response in page_iterator:
        contents = response.get("Contents", [])

        if not contents:
            break

        response_latest_file = max(contents, key=lambda x: x["Key"])

        if latest_file is None:
            latest_file = response_latest_file
        else:
            if response_latest_file["Key"] > latest_file["Key"]:
                latest_file = response_latest_file

    if latest_file is None:
        raise ValueError("No full exports found", full_export_prefix)

    LOGGER.debug("Latest full backup: %s", latest_file)

    # TODO: i think sometimes files get split into multiple pieces and this doesn't work right
    latest_size_bytes = latest_file["Size"]

    # TODO: log how old this full file is

    full_name = latest_file["Key"].split("/")[-1]

    local_file_path = os.path.join(
        settings.target_dir(),
        full_name,
    )

    if os.path.exists(local_file_path):
        LOGGER.debug("%s already exists locally. Skipping download.", local_file_path)
        return local_file_path

    resumable_download(
        s3_client,
        latest_file["Key"],
        local_file_path,
        progress_callback,
        latest_size_bytes,
        settings,
        download_threadpool,
    )

    return local_file_path


def download_incremental(
    download_threadpool: ThreadPoolExecutor,
    s3_client,
    settings: Settings,
    table: Table,
    start_timestamp,
    bytes_downloaded_progress: ProgressCallback,
    empty_steps_progress: ProgressCallback,
):
    """Returns None if the file doesn't exist"""
    end_timestamp = start_timestamp + settings.incremental_duration

    incremental_name = (
        f"{settings.parquet_s3_schema}-{table.name}-{start_timestamp}-{end_timestamp}"
    )

    prefix_name = f"{incremental_name}."
    parquet_name = f"{incremental_name}.parquet"
    empty_name = f"{incremental_name}.empty"

    target_dir = settings.target_dir()

    local_parquet_path = os.path.join(target_dir, parquet_name)
    local_empty_path = os.path.join(target_dir, empty_name)

    # TODO: check if the file is already in the database. if its been fully imported, return now

    if os.path.exists(local_parquet_path):
        LOGGER.debug(
            "%s already exists locally. Skipping download.", local_parquet_path
        )
        return local_parquet_path

    if os.path.exists(local_empty_path):
        LOGGER.debug("%s already exists locally. Skipping download", local_empty_path)
        return local_empty_path

    incremental_s3_prefix = settings.parquet_s3_prefix() + "incremental/"

    # Try downloading with ".parquet" extension first

    # get filesize before downloading for the progress bar
    response = s3_client.list_objects_v2(
        Bucket=settings.parquet_s3_bucket,
        Prefix=incremental_s3_prefix + prefix_name,
    )

    contents = response.get("Contents", [])

    if not contents:
        LOGGER.debug("No files found: %s", incremental_s3_prefix + prefix_name)
        return None

    if len(contents) > 1:
        raise ValueError("Multiple files found", contents)

    head_object = contents[0]

    final_size_bytes = head_object["Size"]

    if final_size_bytes == 0:
        # we should probably check the name, but this seems fine
        empty_steps_progress.more_steps(1)
        return local_empty_path

    resumable_download(
        s3_client,
        head_object["Key"],
        local_parquet_path,
        bytes_downloaded_progress,
        final_size_bytes,
        settings,
        download_threadpool,
    )

    return local_parquet_path


def get_chunk_ranges(
    total_bytes: int, max_chunks: int = 8, min_chunk_size: int = 8 * 1024 * 1024
) -> list[tuple[int, int]]:
    """
    Divide [start_byte, end_byte] into up to `max_chunks` parts,
    each part being at least `min_chunk_size` in length (except possibly the last).
    Returns a list of (start, end) inclusive ranges.
    """
    start_byte = 0
    end_byte = int(total_bytes - 1)

    # Determine chunk size, ensuring we don't go below min_chunk_size
    chunk_size = int(max(min_chunk_size, math.ceil(total_bytes / max_chunks)))

    ranges = []
    current_start = start_byte
    while current_start <= end_byte:
        current_end = int(min(current_start + chunk_size - 1, end_byte))
        ranges.append((current_start, current_end))
        current_start = current_end + 1

    if not ranges:
        raise ValueError("unable to calculate ranges")

    return ranges


def resumable_download(
    s3_client,
    s3_key,
    local_file_path,
    progress_callback,
    final_size_bytes,
    settings: Settings,
    threadpool: ThreadPoolExecutor,
):
    ranges = get_chunk_ranges(final_size_bytes, max_chunks=threadpool._max_workers)

    logging.debug("ranges: %s", ranges)

    incoming_path = local_file_path + ".incoming"

    if len(ranges) == 1:
        _resumable_download_chunk(
            s3_client,
            s3_key,
            incoming_path,
            progress_callback,
            ranges[0][0],
            ranges[0][1],
            settings,
        )
    else:
        fs = [
            threadpool.submit(
                _resumable_download_chunk,
                s3_client,
                s3_key,
                local_file_path + f".incoming{i}",
                progress_callback,
                r[0],
                r[1],
                settings,
            )
            for (i, r) in enumerate(ranges)
        ]

        # make sure local_file_path exists with the right size
        with open(incoming_path, "wb") as wfd:
            # we could do these as completed, but thats more complex
            for f in fs:
                chunk_path = f.result()

                logging.debug("Merging chunk: %s", chunk_path)

                with open(chunk_path, "rb") as rfd:
                    shutil.copyfileobj(rfd, wfd)

    if os.path.getsize(incoming_path) != final_size_bytes:
        raise ValueError("Downloaded file is not the expected size", incoming_path)

    os.rename(incoming_path, local_file_path)

    logging.debug("Finished downloading: %s", local_file_path)


def _resumable_download_chunk(
    s3_client,
    s3_key,
    chunk_path,
    bytes_downloaded_progress,
    chunk_start,
    chunk_end,
    settings: Settings,
):
    final_size = chunk_end - chunk_start + 1

    if os.path.exists(chunk_path):
        start_size = os.path.getsize(chunk_path)
    else:
        start_size = 0

    if start_size > final_size:
        raise ValueError("Downloaded file is larger than expected", chunk_path)

    if start_size < final_size:
        bytes_downloaded_progress.more_steps(final_size - start_size)

        range_header = f"bytes={chunk_start}-{chunk_end}"

        if start_size == 0:
            LOGGER.debug(
                "new download",
                extra={
                    "key": s3_key,
                    "range_header": range_header,
                },
            )
        else:
            LOGGER.debug(
                "resuming download",
                extra={
                    "key": s3_key,
                    "range_header": range_header,
                },
            )

        # TODO: i think this might be slow. i think we need to split this into multiple downloads and run them in parallel
        response = s3_client.get_object(
            Bucket=settings.parquet_s3_bucket,
            Key=s3_key,
            Range=range_header,
        )

        with open(chunk_path, "ab") as f:
            for chunk in response["Body"].iter_chunks(256 * 1024):  # 256KB chunks
                f.write(chunk)
                bytes_downloaded_progress(len(chunk))

    if os.path.getsize(chunk_path) != final_size:
        raise ValueError("Downloaded file is not the expected size", chunk_path)

    LOGGER.info("Finished downloading: %s", chunk_path)

    return chunk_path


def get_s3_client(settings: Settings):
    return _get_s3_client(settings.s3_pool_size)


@cache
def _get_s3_client(max_pool_connections):
    # TODO: read things from Settings to configure this session's profile_name
    session = boto3.Session()

    config = Config(
        retries={
            "max_attempts": 5,
            "mode": "standard",
        },
        max_pool_connections=max_pool_connections,
    )

    client = session.client("s3", config=config)

    # # TODO: poke the s3 connection to make sure its working
    # response = client.get_caller_identity()
    # LOGGER.debug("s3 client identity:", extra={"response": response})

    return client
