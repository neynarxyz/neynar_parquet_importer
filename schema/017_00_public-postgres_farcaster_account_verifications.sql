CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.account_verifications
(
    id bigint PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone,
    fid bigint NOT NULL,
    platform character varying(20) COLLATE pg_catalog."default",
    platform_id text COLLATE pg_catalog."default" NOT NULL,
    platform_username text COLLATE pg_catalog."default" NOT NULL,
    verified_at timestamp without time zone
);

DO $$
BEGIN
    -- Create the index if the table is empty
    IF NOT EXISTS (SELECT 1 FROM ${POSTGRES_SCHEMA}.account_verifications LIMIT 1) THEN
        CREATE INDEX IF NOT EXISTS idx_account_verifications_upsert
        ON ${POSTGRES_SCHEMA}.account_verifications (id, updated_at);
    END IF;
END $$;


CREATE INDEX IF NOT EXISTS account_verifications_fid ON ${POSTGRES_SCHEMA}.account_verifications (fid);
