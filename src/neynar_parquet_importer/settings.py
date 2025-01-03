import logging
import os
import threading
import time
import datadog
from pathlib import Path
from typing import Optional
from pydantic import Field, PostgresDsn
from pydantic_settings import BaseSettings

from neynar_parquet_importer.logger import setup_logging

SHUTDOWN_EVENT = threading.Event()


class Settings(BaseSettings):
    # TODO: whats the best way to take a comma seperated list of tables and convert it to a set? <https://github.com/pydantic/pydantic-settings/issues/291>
    tables: str = ""

    datadog_enabled: bool = True
    exit_after_max_wait: bool = False  # TODO: improve this more
    incremental_duration: int = Field(300, alias="npe_duration")
    interactive_debug: bool = False
    local_input_dir: Path = Path("./data/parquet")
    log_level: str = "INFO"
    log_format: str = "json"
    npe_version: str = "v2"
    parquet_s3_bucket: str = "tf-premium-parquet"
    parquet_s3_database: str = "public-postgres"
    parquet_s3_schema: str = "farcaster"
    postgres_dsn: PostgresDsn = "postgresql+psycopg://postgres:postgres@localhost:15432/example_neynar_parquet_importer"
    postgres_max_overflow: int = 10
    postgres_poolclass: str = "QueuePool"
    postgres_pool_size: int = 50
    postgres_schema: Optional[str] = None
    s3_pool_size: int = 50
    target_name: str = "unknown"

    def initialize(self):
        # TODO: i don't love this, but somewhere in the code is turning naive datetimes into the local timezone instead of UTC
        os.environ["TZ"] = "UTC"
        time.tzset()

        self.setup_datadog()
        self.setup_logging()

    def setup_datadog(self):
        datadog.initialize(
            hostname_from_config=False,
            statsd_constant_tags=[
                f"target:{self.target_name}",
                f"npe_version:{self.npe_version}-{self.incremental_duration}",
                f"parquet_db:{self.parquet_s3_database}",
                f"parquet_schema:{self.parquet_s3_schema}",
            ],
        )

    def setup_logging(self):
        setup_logging(self.log_level, self.log_format)

        logging.getLogger("app").setLevel(self.log_level)

        logging.getLogger("s3transfer").setLevel(logging.INFO)
        logging.getLogger("boto3").setLevel(logging.INFO)
        logging.getLogger("botocore").setLevel(logging.INFO)
        logging.getLogger("urllib3").setLevel(logging.INFO)

        if self.datadog_enabled:
            logging.getLogger("datadog.dogstatsd").setLevel(logging.INFO)
        else:
            logging.getLogger("datadog.dogstatsd").setLevel(100)

    def parquet_s3_prefix(self):
        prefix = (
            f"{self.parquet_s3_database}/{self.parquet_s3_schema}/{self.npe_version}/"
        )

        if self.npe_version == "v2":
            assert self.incremental_duration == 300
        elif self.npe_version == "v3":
            prefix += f"{self.incremental_duration}/"

        return prefix

    def target_dir(self):
        return (
            self.local_input_dir / self.npe_version / self.parquet_s3_database
            # don't include the schema. it's alredy in the filename
            # / self.parquet_s3_schema
        )
