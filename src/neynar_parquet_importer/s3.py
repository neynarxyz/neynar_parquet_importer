from functools import cache, lru_cache
import logging
import os
import re
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
        s3_prefix,
        full_name,
        local_file_path,
        progress_callback,
        latest_size_bytes,
        settings,
    )

    return local_file_path


def download_incremental(
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

    latest_size_bytes = head_object["Size"]

    if latest_size_bytes == 0:
        # we should probably check the name, but this seems fine
        empty_steps_progress.more_steps(1)
        return local_empty_path

    resumable_download(
        s3_client,
        incremental_s3_prefix,
        parquet_name,
        local_parquet_path,
        bytes_downloaded_progress,
        latest_size_bytes,
        settings,
    )

    return local_parquet_path


def resumable_download(
    s3_client,
    s3_prefix,
    s3_key,
    target_path,
    bytes_downloaded_progress,
    source_size_bytes,
    settings: Settings,
):
    incoming_path = target_path + ".incoming"

    if os.path.exists(incoming_path):
        incoming_size = os.path.getsize(incoming_path)
    else:
        incoming_size = 0

    if incoming_size > source_size_bytes:
        raise ValueError("Downloaded file is larger than expected", incoming_path)

    if incoming_size < source_size_bytes:
        bytes_downloaded_progress.more_steps(source_size_bytes - incoming_size)

        range_header = f"bytes={incoming_size}-{source_size_bytes}"

        key = s3_prefix + s3_key

        if source_size_bytes == 0:
            logging.debug(
                "new download",
                extra={
                    "key": key,
                },
            )
        else:
            logging.debug(
                "resuming download",
                extra={
                    "key": key,
                    "range_header": range_header,
                },
            )

        response = s3_client.get_object(
            Bucket=settings.parquet_s3_bucket,
            Key=key,
            Range=range_header,
        )

        # TODO: resumable downloads! use download_file_obj?
        with open(incoming_path, "ab") as f:
            for chunk in response["Body"].iter_chunks(1024 * 1024):  # 1MB chunks
                f.write(chunk)
                bytes_downloaded_progress(len(chunk))

    if os.path.getsize(incoming_path) != source_size_bytes:
        raise ValueError("Downloaded file is not the expected size", incoming_path)

    os.rename(incoming_path, target_path)

    LOGGER.info("Downloaded: %s", target_path)


def get_s3_client(settings: Settings):
    return _get_s3_client(settings.s3_pool_size)


@lru_cache(maxsize=None)
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
