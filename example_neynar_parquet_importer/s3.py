import os
import boto3
import logging

# TODO: get this from env var
LOCAL_FULL_DIR = "./data/parquet/full"
LOCAL_INCREMENTAL_DIR = "./data/parquet/incremental"

S3_CLIENT = boto3.client("s3")
BUCKET_NAME = "tf-premium-parquet"

PARQUET_S3_PREFIX = f"public-postgres/farcaster/v2"

FULL_PARQUET_S3_URI = PARQUET_S3_PREFIX + "/full"
INCREMENTAL_PARQUET_S3_URI = PARQUET_S3_PREFIX + "/incremental"


def download_latest_full(table_name):
    if not os.path.exists(LOCAL_FULL_DIR):
        os.makedirs(LOCAL_FULL_DIR)

    logging.info("Downloading the latest full backup...")

    # TODO: look up the actual latest file
    full_name = f"farcaster-{table_name}-0-1724173200.parquet"

    local_file_path = os.path.join(LOCAL_FULL_DIR, full_name)

    if os.path.exists(local_file_path):
        logging.info("%s already exists locally. Skipping download.", local_file_path)
        return local_file_path

    logging.info("Downloading to %s...", local_file_path)
    S3_CLIENT.download_file(
        BUCKET_NAME, FULL_PARQUET_S3_URI + "/" + full_name, local_file_path
    )

    return local_file_path


def download_incrementals(tablename, start_timestamp, end_timestamp):
    logging.info("Downloading the incrementals...")

    raise NotImplementedError


def download_incremental(tablename, start_timestamp, duration):
    end_timestamp = start_timestamp + duration

    incremental_name = f"farcaster-{tablename}-{start_timestamp}-{end_timestamp}"

    logging.info("Downloading the incremental for %s...", incremental_name)

    any_name = f"{incremental_name}.*"
    parquet_name = f"{incremental_name}.parquet"
    empty_name = f"{incremental_name}.empty"

    # TODO: download the any_name file and check if its empty or parquet

    raise NotImplementedError
