CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.follows
(
    id UUID PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    "timestamp" timestamp without time zone NOT NULL,
    fid bigint NOT NULL,
    target_fid bigint NOT NULL
);

DO $$
BEGIN
    -- Add new columns to follows if they don't already exist
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'follows'
          AND table_schema = '${POSTGRES_SCHEMA}'
          AND column_name = 'display_timestamp'
    ) THEN
        ALTER TABLE ${POSTGRES_SCHEMA}.follows
        ADD COLUMN display_timestamp TIMESTAMP;
    END IF;

    -- add creator_app_fid and deleter_app_fid columns if they don't already exist
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'follows'
        AND table_schema = '${POSTGRES_SCHEMA}'
        AND column_name = 'creator_app_fid'
    ) THEN
        ALTER TABLE ${POSTGRES_SCHEMA}.follows
        ADD COLUMN creator_app_fid bigint;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'follows'
        AND table_schema = '${POSTGRES_SCHEMA}'
        AND column_name = 'deleter_app_fid'
    ) THEN
        ALTER TABLE ${POSTGRES_SCHEMA}.follows
        ADD COLUMN deleter_app_fid bigint;
    END IF;

    -- Create the index if the table is empty
    IF NOT EXISTS (SELECT 1 FROM ${POSTGRES_SCHEMA}.follows LIMIT 1) THEN
        CREATE INDEX IF NOT EXISTS idx_follows_upsert
        ON ${POSTGRES_SCHEMA}.follows (id, updated_at);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS follows_fid ON ${POSTGRES_SCHEMA}.follows (fid);
CREATE INDEX IF NOT EXISTS follows_target_fid ON ${POSTGRES_SCHEMA}.follows (target_fid);
CREATE INDEX IF NOT EXISTS follows_timestamp_not_deleted ON ${POSTGRES_SCHEMA}.follows ("timestamp") WHERE deleted_at IS NULL;
