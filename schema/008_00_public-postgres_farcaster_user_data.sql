CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.user_data
(
    id bigint PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    "timestamp" timestamp without time zone NOT NULL,
    fid bigint NOT NULL,
    "hash" bytea NOT NULL UNIQUE,
    "type" smallint NOT NULL,
    "value" text COLLATE pg_catalog."default" NOT NULL
);

DO $$
BEGIN
    -- Check if the constraint exists
    IF EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_name = 'user_data'
          AND table_schema = '${POSTGRES_SCHEMA}'
          AND constraint_name = 'user_data_fid_type_unique'
          AND constraint_type = 'UNIQUE'
    ) THEN
        -- Drop the constraint
        ALTER TABLE user_data DROP CONSTRAINT user_data_fid_type_unique;
    END IF;

    -- Create the index if the table is empty
    IF NOT EXISTS (SELECT 1 FROM ${POSTGRES_SCHEMA}.user_data LIMIT 1) THEN
        CREATE INDEX IF NOT EXISTS idx_user_data_upsert
        ON ${POSTGRES_SCHEMA}.user_data (id, updated_at);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS user_data_fid ON ${POSTGRES_SCHEMA}.user_data (fid);
CREATE INDEX IF NOT EXISTS user_data_timestamp_not_deleted ON ${POSTGRES_SCHEMA}.user_data ("timestamp") WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS user_data_type ON ${POSTGRES_SCHEMA}.user_data (type);
