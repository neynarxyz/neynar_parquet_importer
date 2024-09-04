import os
import boto3
import botocore.exceptions
from botocore.config import Config

from neynar_parquet_importer.app import (
    LOGGER,
)

# TODO: get this from env var
LOCAL_FULL_DIR = "./data/parquet/full"
LOCAL_INCREMENTAL_DIR = "./data/parquet/incremental"

S3_CLIENT = boto3.client("s3", config=Config(max_pool_connections=50))
BUCKET_NAME = "tf-premium-parquet"

PARQUET_S3_PREFIX = "public-postgres/farcaster/v2"

FULL_PARQUET_S3_URI = PARQUET_S3_PREFIX + "/full"
INCREMENTAL_PARQUET_S3_URI = PARQUET_S3_PREFIX + "/incremental"

if not os.path.exists(LOCAL_FULL_DIR):
    os.makedirs(LOCAL_FULL_DIR)

if not os.path.exists(LOCAL_INCREMENTAL_DIR):
    os.makedirs(LOCAL_INCREMENTAL_DIR)


def download_latest_full(table_name, progress_callback):
    response = S3_CLIENT.list_objects_v2(
        Bucket=BUCKET_NAME,
        Prefix=FULL_PARQUET_S3_URI + f"/farcaster-{table_name}-0-",
    )

    latest_file = max(response["Contents"], key=lambda x: x["Key"].split("/")[-1])

    latest_size_bytes = latest_file["Size"]

    # TODO: log how old this full file is

    full_name = latest_file["Key"].split("/")[-1]

    local_file_path = os.path.join(LOCAL_FULL_DIR, full_name)

    if os.path.exists(local_file_path):
        LOGGER.debug("%s already exists locally. Skipping download.", local_file_path)
        return local_file_path

    progress_callback.more_steps(latest_size_bytes)

    LOGGER.info("Downloading the latest full backup to %s...", local_file_path)
    S3_CLIENT.download_file(
        BUCKET_NAME,
        FULL_PARQUET_S3_URI + "/" + full_name,
        local_file_path,
        Callback=progress_callback,
    )

    return local_file_path


def download_incremental(tablename, start_timestamp, duration, progress_callback):
    end_timestamp = start_timestamp + duration

    incremental_name = f"farcaster-{tablename}-{start_timestamp}-{end_timestamp}"

    parquet_name = f"{incremental_name}.parquet"
    empty_name = f"{incremental_name}.empty"

    local_parquet_path = os.path.join(LOCAL_INCREMENTAL_DIR, parquet_name)
    local_empty_path = os.path.join(LOCAL_INCREMENTAL_DIR, empty_name)

    # TODO: check if the file is already in the database. if its been fully imported, return now

    if os.path.exists(local_parquet_path):
        LOGGER.debug(
            "%s already exists locally. Skipping download.", local_parquet_path
        )
        return local_parquet_path

    if os.path.exists(local_empty_path):
        LOGGER.debug("%s already exists locally. Skipping download", local_empty_path)
        return local_empty_path

    # Try downloading with ".parquet" extension first
    try:
        # TODO: get filesize before downloading for the progress bar
        latest_size_bytes = S3_CLIENT.head_object(
            Bucket=BUCKET_NAME, Key=INCREMENTAL_PARQUET_S3_URI + "/" + parquet_name
        )["ContentLength"]

        progress_callback.more_steps(latest_size_bytes)

        # TODO: callback on this download to update the progress bar
        S3_CLIENT.download_file(
            BUCKET_NAME,
            INCREMENTAL_PARQUET_S3_URI + "/" + parquet_name,
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

    # If ".parquet" file doesn't exist, try with ".empty"
    try:
        S3_CLIENT.download_file(
            BUCKET_NAME,
            INCREMENTAL_PARQUET_S3_URI + "/" + empty_name,
            local_empty_path,
        )
        LOGGER.debug("Downloaded empty: %s", local_empty_path)

        return local_empty_path
    except botocore.exceptions.ClientError as e:
        if e.response["Error"]["Code"] == "404":
            pass
        else:
            raise

    return None
