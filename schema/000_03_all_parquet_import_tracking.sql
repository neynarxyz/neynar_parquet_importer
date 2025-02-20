CREATE INDEX IF NOT EXISTS idx_parquet_import_tracking_end_timestamp ON ${POSTGRES_SCHEMA}.parquet_import_tracking(end_timestamp);
