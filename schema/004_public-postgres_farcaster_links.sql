CREATE TABLE IF NOT EXISTS links
(
    id bigint PRIMARY KEY,
    fid bigint,
    target_fid bigint,
    "hash" bytea NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    "type" text COLLATE pg_catalog."default",
    display_timestamp timestamp without time zone,
    CONSTRAINT links_fid_target_fid_type_unique UNIQUE (fid, target_fid, type),
    CONSTRAINT links_hash_unique UNIQUE (hash)
);

-- TODO: add indexes to the tables as needed
