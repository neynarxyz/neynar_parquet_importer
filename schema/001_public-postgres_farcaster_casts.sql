CREATE TABLE IF NOT EXISTS casts
(
    id bigint PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    "timestamp" timestamp without time zone NOT NULL,
    fid bigint NOT NULL,
    "hash" bytea NOT NULL,
    parent_hash bytea,
    parent_fid bigint,
    parent_url text COLLATE pg_catalog."default",
    "text" text COLLATE pg_catalog."default" NOT NULL,
    embeds jsonb NOT NULL DEFAULT '{}'::jsonb,
    mentions bigint[] NOT NULL DEFAULT '{}'::bigint[],
    mentions_positions smallint[] NOT NULL DEFAULT '{}'::smallint[],
    root_parent_hash bytea,
    root_parent_url text COLLATE pg_catalog."default",
    CONSTRAINT casts_hash_unique UNIQUE (hash)
);

-- TODO: add indexes to the tables as needed
