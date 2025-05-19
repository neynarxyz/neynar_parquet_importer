CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.channels
(
    id bigint PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone,
    channel_id text COLLATE pg_catalog."default" NOT NULL,
    url text COLLATE pg_catalog."default" NOT NULL,
    description text COLLATE pg_catalog."default" NOT NULL,
    image_url text COLLATE pg_catalog."default" NOT NULL,
    lead_fid bigint NOT NULL,
    moderator_fids bigint[] NOT NULL,
    follower_count integer NOT NULL,
    "timestamp" timestamp without time zone NOT NULL
);

DO $$
BEGIN
    -- Create the index if the table is empty
    IF NOT EXISTS (SELECT 1 FROM ${POSTGRES_SCHEMA}.channels LIMIT 1) THEN
        CREATE INDEX IF NOT EXISTS idx_channels_upsert
        ON ${POSTGRES_SCHEMA}.channels (id, updated_at);
    END IF;
END $$;


CREATE INDEX IF NOT EXISTS channels_channel_id ON ${POSTGRES_SCHEMA}.channels (channel_id);
CREATE INDEX IF NOT EXISTS channels_lead_fid ON ${POSTGRES_SCHEMA}.channels (lead_fid);
CREATE INDEX IF NOT EXISTS channels_timestamp_not_deleted ON ${POSTGRES_SCHEMA}.channels ("timestamp") WHERE deleted_at IS NULL;
