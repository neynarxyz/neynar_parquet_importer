from functools import lru_cache
import os
import re
import boto3
import botocore.exceptions
from botocore.config import Config

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


def download_latest_full(s3_client, settings: Settings, table_name, progress_callback):
    s3_prefix = settings.parquet_s3_prefix() + "full/"

    full_export_prefix = s3_prefix + f"{settings.parquet_s3_schema}-{table_name}-0-"

    paginator = s3_client.get_paginator("list_objects_v2")
    operation_parameters = {
        "Bucket": settings.parquet_s3_bucket,
        "Prefix": full_export_prefix,
    }
    page_iterator = paginator.paginate(**operation_parameters)
    latest_file = None
    for response in page_iterator:
        response_latest_file = max(response.get("Contents", []), key=lambda x: x["Key"])

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

    progress_callback.more_steps(latest_size_bytes)

    LOGGER.info("Downloading the latest full backup to %s...", local_file_path)
    s3_client.download_file(
        settings.parquet_s3_bucket,
        s3_prefix + full_name,
        local_file_path,
        Callback=progress_callback,
    )

    return local_file_path


def download_incremental(
    s3_client,
    settings: Settings,
    tablename,
    start_timestamp,
    progress_callback,
    empty_callback,
):
    """Returns None if the file doesn't exist"""
    end_timestamp = start_timestamp + settings.incremental_duration

    incremental_name = (
        f"{settings.parquet_s3_schema}-{tablename}-{start_timestamp}-{end_timestamp}"
    )

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
    # TODO: one s3_client command to get both .parquet and .empty? and maybe even the .schema?
    try:
        # TODO: get filesize before downloading for the progress bar
        latest_size_bytes = s3_client.head_object(
            Bucket=settings.parquet_s3_bucket,
            Key=incremental_s3_prefix + parquet_name,
        )["ContentLength"]

        progress_callback.more_steps(latest_size_bytes)

        # TODO: callback on this download to update the progress bar
        s3_client.download_file(
            settings.parquet_s3_bucket,
            incremental_s3_prefix + parquet_name,
            local_parquet_path,
            Callback=progress_callback,
        )
        LOGGER.info("Downloaded: %s", local_parquet_path)

        return local_parquet_path
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            pass
        else:
            raise

    # If ".parquet" file doesn't exist, check for ".empty"
    # we don't actually download it because it's empty
    try:
        s3_client.head_object(
            Bucket=settings.parquet_s3_bucket,
            Key=incremental_s3_prefix + empty_name,
        )["ContentLength"]

        empty_callback.more_steps(1)

        return local_empty_path
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            pass
        else:
            raise

    return None


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
