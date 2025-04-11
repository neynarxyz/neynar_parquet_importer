-- NOTE! This table name will conflict with the v2 reactions table!

CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.reactions
(
    id uuid NOT NULL PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone,
    "timestamp" timestamp without time zone NOT NULL,
    reaction_type smallint NOT NULL,
    fid bigint NOT NULL,
    hash bytea NOT NULL,
    target_hash bytea,
    target_fid bigint,
    target_url text COLLATE pg_catalog."default"
)
