CREATE INDEX IF NOT EXISTS idx_parquet_import_tracking_imported_at ON ${POSTGRES_SCHEMA}.parquet_import_tracking(imported_at);
