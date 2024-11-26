CREATE TABLE IF NOT EXISTS nindexer_verifications
(
    id bigint PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    "timestamp" timestamp without time zone NOT NULL,
    "address" bytea NOT NULL,
    fid bigint NOT NULL,
    protocol smallint NOT NULL
);

-- TODO: add indexes to the tables as needed

-- TODO: more tables! theres more nindexer migrations to write!