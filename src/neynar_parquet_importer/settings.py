from pathlib import Path
from typing import Optional
from pydantic import PostgresDsn
from pydantic_settings import BaseSettings


class Settings(BaseSettings):
    # TODO: whats the best way to take a comma seperated list of tables and convert it to a set? <https://github.com/pydantic/pydantic-settings/issues/291>
    tables: str = ""

    incremental_duration: int = 300
    interactive_debug: bool = False
    local_full_dir: Path = Path("./data/parquet/full")
    local_incremental_dir: Path = Path("./data/parquet/incremental")
    npe_version: str = "v2"
    parquet_s3_bucket = "tf-premium-parquet"
    parquet_s3_database = "public-postgres"
    parquet_s3_schema = "farcaster"
    postgres_dsn: PostgresDsn = "postgresql+psycopg2://postgres:postgres@postgres:5432/example_neynar_parquet_importer"
    postgres_pool_size: int = 50
    postgres_schema: Optional[str] = None
    s3_pool_size: int = 50

    def parquet_s3_prefix(self):
        prefix = (
            f"{self.parquet_s3_database}/{self.parquet_s3_schema}/{self.npe_version}/"
        )

        if self.npe_version == "v2":
            assert self.incremental_duration == 300
        elif self.npe_version == "v3":
            prefix += f"{self.incremental_duration}/"

        return prefix
