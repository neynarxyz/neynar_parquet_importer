from pydantic import PostgresDsn
from pydantic_settings import BaseSettings


# TODO: if npe_version is v2, duration MUST be 300
class Settings(BaseSettings):
    # TODO: whats the best way to take a comma seperated list of tables and convert it to a set? <https://github.com/pydantic/pydantic-settings/issues/291>
    tables: str = ""

    incremental_duration: int = 300
    interactive_debug: bool = False
    npe_version: str = "v2"
    postgres_dsn: PostgresDsn = "postgresql+psycopg2://postgres:postgres@postgres:5432/example_neynar_parquet_importer"
    postgres_pool_size: int = 30
