CREATE TABLE IF NOT EXISTS parquet_import_tracking (
    id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR NOT NULL,
    file_name VARCHAR UNIQUE,
    file_type VARCHAR NOT NULL,
    file_version VARCHAR NOT NULL,
    file_duration_s INT NOT NULL,
    is_empty BOOLEAN,
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_row_group_imported INT DEFAULT NULL,
    total_row_groups INT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_parquet_import_tracking_table_name_and_version ON parquet_import_tracking(table_name, file_version, file_duration_s);
CREATE INDEX IF NOT EXISTS idx_parquet_import_tracking_file_type ON parquet_import_tracking(file_type);
CREATE INDEX IF NOT EXISTS idx_parquet_import_tracking_imported_at ON parquet_import_tracking(imported_at);