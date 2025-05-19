-- NOTE! This table name will conflict with the v2 casts table!

CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.casts
(
    id uuid NOT NULL PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone,
    "timestamp" timestamp without time zone NOT NULL,
    fid bigint NOT NULL,
    hash bytea NOT NULL,
    text text COLLATE pg_catalog."default" NOT NULL,
    parent_hash bytea,
    parent_fid bigint,
    parent_url text COLLATE pg_catalog."default",
    root_parent_hash bytea NOT NULL,
    root_parent_url text COLLATE pg_catalog."default",
    embedded_urls text[] COLLATE pg_catalog."default",
    embedded_casts bytea[],
    mentions bigint[],
    mentions_positions smallint[],
    ticker_mentions text[] COLLATE pg_catalog."default",
    ticker_mentions_positions smallint[],
    channel_mentions text[] COLLATE pg_catalog."default",
    channel_mentions_positions smallint[],
    embedded_casts_fids bigint[],
    embeds jsonb,
    creator_app_fid bigint,
    deleter_app_fid bigint
);

CREATE INDEX IF NOT EXISTS casts_hash ON ${POSTGRES_SCHEMA}.casts ("hash");
