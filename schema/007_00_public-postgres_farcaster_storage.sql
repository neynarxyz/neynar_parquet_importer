CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.storage
(
    id bigint PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    "timestamp" timestamp without time zone NOT NULL,
    fid bigint NOT NULL,
    units bigint NOT NULL,
    expiry timestamp without time zone NOT NULL
);

DO $$
BEGIN
    -- Check if the constraint exists
    IF EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_name = 'storage'
          AND table_schema = '${POSTGRES_SCHEMA}'
          AND constraint_name = 'unique_fid_units_expiry'
          AND constraint_type = 'UNIQUE'
    ) THEN
        -- Drop the constraint
        ALTER TABLE ${POSTGRES_SCHEMA}.storage DROP CONSTRAINT unique_fid_units_expiry;
    END IF;

    -- Create the index if the table is empty
    IF NOT EXISTS (SELECT 1 FROM ${POSTGRES_SCHEMA}.storage LIMIT 1) THEN
        CREATE INDEX IF NOT EXISTS idx_storage_upsert
        ON ${POSTGRES_SCHEMA}.storage (id, updated_at);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS storage_fid ON ${POSTGRES_SCHEMA}.storage (fid);
CREATE INDEX IF NOT EXISTS storage_timestamp_not_deleted ON ${POSTGRES_SCHEMA}.storage ("timestamp") WHERE deleted_at IS NULL;
