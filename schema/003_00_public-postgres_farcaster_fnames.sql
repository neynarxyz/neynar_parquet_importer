CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.fnames
(
    fname text COLLATE pg_catalog."default" NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    custody_address bytea,
    expires_at timestamp without time zone,
    fid bigint,
    deleted_at timestamp without time zone,
    CONSTRAINT fnames_pkey PRIMARY KEY (fname)
);

DO $$
BEGIN
    -- Create the index if the table is empty
    IF NOT EXISTS (SELECT 1 FROM ${POSTGRES_SCHEMA}.fnames LIMIT 1) THEN
        CREATE INDEX IF NOT EXISTS idx_fnames_upsert
        ON fnames (id, updated_at);
    END IF;
END $$;


CREATE INDEX IF NOT EXISTS fnames_fid ON ${POSTGRES_SCHEMA}.fnames (fid);
