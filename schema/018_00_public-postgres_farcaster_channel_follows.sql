CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.channel_follows
(
    id bigint PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone,
    fid bigint NOT NULL,
    channel_id text COLLATE pg_catalog."default" NOT NULL,
    "timestamp" timestamp without time zone NOT NULL
);

DO $$
BEGIN
    -- Check if the constraint exists
    IF EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_name = 'channel_follows'
          AND table_schema = '${POSTGRES_SCHEMA}'
          AND constraint_name = 'channel_follows_fid_channel_id_unique'
          AND constraint_type = 'UNIQUE'
    ) THEN
        -- Drop the constraint
        ALTER TABLE ${POSTGRES_SCHEMA}.channel_follows DROP CONSTRAINT channel_follows_fid_channel_id_unique;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS channel_follows_fid ON ${POSTGRES_SCHEMA}.channel_follows (fid);
CREATE INDEX IF NOT EXISTS channel_follows_channel_id ON ${POSTGRES_SCHEMA}.channel_follows (channel_id);
CREATE INDEX IF NOT EXISTS channel_follows_timestamp_not_deleted ON ${POSTGRES_SCHEMA}.channel_follows ("timestamp") WHERE deleted_at IS NULL;
