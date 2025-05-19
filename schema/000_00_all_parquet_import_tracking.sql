CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.parquet_import_tracking (
    id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR NOT NULL,
    file_name VARCHAR UNIQUE,
    file_type VARCHAR NOT NULL,
    file_version VARCHAR NOT NULL,
    file_duration_s INT NOT NULL,
    is_empty BOOLEAN,
    end_timestamp TIMESTAMP,
    last_row_group_imported INT DEFAULT NULL,
    total_row_groups INT NOT NULL
);

DO $$
BEGIN
    -- Add new columns to parquet_import_tracking if they don't already exist
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'parquet_import_tracking'
        AND table_schema = '${POSTGRES_SCHEMA}'
        AND column_name = 'completed'
    ) THEN
        ALTER TABLE ${POSTGRES_SCHEMA}.parquet_import_tracking
        ADD COLUMN completed BOOLEAN DEFAULT TRUE;

        ALTER TABLE ${POSTGRES_SCHEMA}.parquet_import_tracking
        ALTER COLUMN completed SET DEFAULT FALSE;
    END IF;
END $$;

DO $$
BEGIN
    -- Rename imported_at to end_timestamp on parquet_import_tracking
    IF EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'parquet_import_tracking'
        AND table_schema = '${POSTGRES_SCHEMA}'
        AND column_name = 'imported_at'
    ) THEN
        ALTER TABLE ${POSTGRES_SCHEMA}.parquet_import_tracking
        RENAME COLUMN imported_at TO end_timestamp;

        ALTER TABLE ${POSTGRES_SCHEMA}.parquet_import_tracking
        ALTER COLUMN end_timestamp DROP DEFAULT;

        ALTER INDEX ${POSTGRES_SCHEMA}.idx_parquet_import_tracking_imported_at
        RENAME TO idx_parquet_import_tracking_end_timestamp;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_parquet_import_tracking_table_name_and_version ON ${POSTGRES_SCHEMA}.parquet_import_tracking(table_name, file_version, file_duration_s);
CREATE INDEX IF NOT EXISTS idx_parquet_import_tracking_file_type ON ${POSTGRES_SCHEMA}.parquet_import_tracking(file_type);
CREATE INDEX IF NOT EXISTS idx_parquet_import_tracking_end_timestamp ON ${POSTGRES_SCHEMA}.parquet_import_tracking(end_timestamp);
