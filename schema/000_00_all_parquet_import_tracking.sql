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

DO $$
BEGIN
    -- Add new columns to parquet_import_tracking if they don't already exist
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'parquet_import_tracking' AND column_name = 'completed'
    ) THEN
        ALTER TABLE parquet_import_tracking
        ADD COLUMN completed BOOLEAN DEFAULT TRUE;

        ALTER TABLE parquet_import_tracking
        ALTER COLUMN completed SET DEFAULT FALSE;
    END IF;
END $$;
