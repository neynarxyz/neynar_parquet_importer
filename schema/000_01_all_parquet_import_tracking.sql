CREATE INDEX IF NOT EXISTS idx_parquet_import_tracking_table_name_and_version ON ${POSTGRES_SCHEMA}.parquet_import_tracking(table_name, file_version, file_duration_s);
