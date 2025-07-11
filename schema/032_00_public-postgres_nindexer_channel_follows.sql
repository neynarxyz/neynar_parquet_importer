CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.channel_follows
(
    id uuid NOT NULL PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone,
    fid bigint NOT NULL,
    channel_id text COLLATE pg_catalog."default" NOT NULL,
    "timestamp" timestamp without time zone NOT NULL
);

CREATE INDEX IF NOT EXISTS channel_follows_fid ON ${POSTGRES_SCHEMA}.channel_follows (fid);
CREATE INDEX IF NOT EXISTS channel_follows_channel_id ON ${POSTGRES_SCHEMA}.channel_follows (channel_id);
CREATE INDEX IF NOT EXISTS channel_follows_timestamp_not_deleted ON ${POSTGRES_SCHEMA}.channel_follows ("timestamp") WHERE deleted_at IS NULL;
