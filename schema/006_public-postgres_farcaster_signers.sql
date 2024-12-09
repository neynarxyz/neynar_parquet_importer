CREATE TABLE IF NOT EXISTS signers
(
    id bigint PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    "timestamp" timestamp without time zone NOT NULL,
    fid bigint NOT NULL,
    "hash" bytea,
    custody_address bytea,
    signer bytea NOT NULL,
    "name" text COLLATE pg_catalog."default",
    app_fid bigint,
    CONSTRAINT unique_timestamp_fid_signer UNIQUE ("timestamp", fid, signer)
);

-- TODO: add indexes to the tables as needed
