CREATE TABLE IF NOT EXISTS user_data
(
    id bigint PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    "timestamp" timestamp without time zone NOT NULL,
    fid bigint NOT NULL,
    "hash" bytea NOT NULL UNIQUE,
    "type" smallint NOT NULL,
    "value" text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT user_data_fid_type_unique UNIQUE (fid, type)
);

-- TODO: add indexes to the tables as needed
