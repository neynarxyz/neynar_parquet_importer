import os
import boto3
import botocore.exceptions
from botocore.config import Config

from neynar_parquet_importer.app import (
    LOGGER,
)
from neynar_parquet_importer.settings import Settings


def download_latest_full(s3_client, settings: Settings, table_name, progress_callback):
    s3_prefix = settings.parquet_s3_prefix() + "full/"

    response = s3_client.list_objects_v2(
        Bucket=settings.parquet_s3_bucket,
        Prefix=s3_prefix + f"{settings.parquet_s3_schema}-{table_name}-0-",
    )

    latest_file = max(response["Contents"], key=lambda x: x["Key"].split("/")[-1])

    latest_size_bytes = latest_file["Size"]

    # TODO: log how old this full file is

    full_name = latest_file["Key"].split("/")[-1]

    local_file_path = os.path.join(settings.local_full_dir, full_name)

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
    end_timestamp = start_timestamp + settings.incremental_duration

    incremental_name = (
        f"{settings.parquet_s3_schema}-{tablename}-{start_timestamp}-{end_timestamp}"
    )

    parquet_name = f"{incremental_name}.parquet"
    empty_name = f"{incremental_name}.empty"

    local_parquet_path = os.path.join(settings.local_incremental_dir, parquet_name)
    local_empty_path = os.path.join(settings.local_incremental_dir, empty_name)

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
    return boto3.client("s3", config=Config(max_pool_connections=settings.s3_pool_size))
