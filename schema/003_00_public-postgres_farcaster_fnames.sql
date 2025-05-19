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

CREATE INDEX IF NOT EXISTS fnames_fid ON ${POSTGRES_SCHEMA}.fnames (fid);
