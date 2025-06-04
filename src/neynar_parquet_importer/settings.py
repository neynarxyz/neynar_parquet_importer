import logging
import os
import threading
import time
import datadog
from pathlib import Path
from pydantic import Field, PostgresDsn
from pydantic_settings import BaseSettings, SettingsConfigDict
from enum import Enum

from .logger import setup_logging
from .neynar_api import NeynarApiClient

SHUTDOWN_EVENT = threading.Event()


class CuMode(str, Enum):
    OFF = "off"
    ON = "on"
    SHADOW = "shadow"

    def metric(self) -> str | None:
        if self == CuMode.ON:
            return "usage.cu"
        elif self == CuMode.SHADOW:
            return "shadow.usage.cu"
        elif self == CuMode.OFF:
            return None


class ShuttingDown(Exception):
    pass


class Settings(BaseSettings):
    # TODO: whats the best way to take a comma seperated list of tables and convert it to a set? <https://github.com/pydantic/pydantic-settings/issues/291>
    tables: str = ""
    views: str = ""

    app_uuid: str | None = None
    cu_mode: CuMode = CuMode.OFF
    datadog_enabled: bool = True
    download_workers: int = 32
    exit_after_max_wait: bool = False  # TODO: improve this more
    file_workers: int = 4
    filtered_row_multiplier: float = 1.1
    filter_file: Path | None = None
    incremental_duration: int = Field(300, alias="npe_duration")
    interactive_debug: bool = False
    local_input_dir: Path = Path("./data/parquet")
    local_input_only: bool = False  # useful for development
    log_format: str = "json"
    log_level: str = "INFO"
    neynar_api_key: str | None = None
    neynar_api_url: str = "https://api.neynar.com"
    npe_version: str = "v2"
    parquet_s3_bucket: str = "tf-premium-parquet"
    parquet_s3_database: str = "public-postgres"
    parquet_s3_schema: str = "farcaster"
    postgres_dsn: PostgresDsn = PostgresDsn(
        "postgresql+psycopg://postgres:postgres@localhost:15432/example_neynar_parquet_importer"
    )
    postgres_max_overflow: int = 10
    postgres_pool_size: int = 90
    postgres_poolclass: str = "QueuePool"
    postgres_schema: str = "public"
    row_workers: int = 6
    skip_full_import: bool = False
    s3_pool_size: int = 100
    target_name: str = "unknown"

    model_config = SettingsConfigDict(
        env_file=os.environ.get("ENV_FILE", ".env"),
        env_file_encoding="utf-8",
        extra="allow",
    )

    def initialize(self):
        if not self.postgres_schema:
            self.postgres_schema = "public"

        if not self.npe_version:
            self.npe_version = "v2"

        if not self.incremental_duration:
            if self.npe_version == "v2":
                self.incremental_duration = 300
            elif self.npe_version == "v3":
                self.incremental_duration = 1
            else:
                raise ValueError("no incremental duration set!")

        # TODO: i don't love this, but somewhere in the code is turning naive datetimes into the local timezone instead of UTC
        os.environ["TZ"] = "UTC"
        time.tzset()

        incoming_dir = self.incoming_dir()
        incoming_dir.mkdir(parents=True, exist_ok=True)

        target_dir = self.target_dir()
        target_dir.mkdir(parents=True, exist_ok=True)

        self.setup_datadog()
        self.setup_logging()

        logging.info("some settings", extra={"cu_mode": self.cu_mode})

    def setup_datadog(self):
        statsd_constant_tags = [
            f"target:{self.target_name}",
            f"npe_version:{self.npe_version}-{self.incremental_duration}",
            f"parquet_db:{self.parquet_s3_database}",
            f"parquet_schema:{self.parquet_s3_schema}",
        ]

        if self.app_uuid:
            statsd_constant_tags.append(f"app_uuid:{self.app_uuid}")

        datadog.initialize(
            hostname_from_config=False,
            statsd_constant_tags=statsd_constant_tags,
        )

    def setup_logging(self):
        # TODO: we should have a logger that sends to statsd so it keeps the tags. the tags get lost
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

    def parquet_s3_prefix(self) -> str:
        prefix = (
            f"{self.parquet_s3_database}/{self.parquet_s3_schema}/{self.npe_version}/"
        )

        if self.npe_version == "v2":
            assert self.incremental_duration == 300
        elif self.npe_version == "v3":
            prefix += f"{self.incremental_duration}/"

        return prefix

    def target_dir(self) -> Path:
        return (
            self.local_input_dir / self.npe_version / self.parquet_s3_database
            # don't include the schema. it's alredy in the filename
            # / self.parquet_s3_schema
        )

    def incoming_dir(self) -> Path:
        return self.target_dir() / f".incoming.{self.target_name}"

    def neynar_api_client(self) -> NeynarApiClient:
        return NeynarApiClient.new(
            api_key=self.neynar_api_key,
            api_url=self.neynar_api_url,
        )
