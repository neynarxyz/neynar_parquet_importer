CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.usernames
(
    id uuid NOT NULL PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone,
    username text COLLATE pg_catalog."default" NOT NULL,
    fid integer NOT NULL,
    custody_address bytea,
    proof_timestamp timestamp without time zone NOT NULL,
    type smallint NOT NULL
);

DO $$
BEGIN
    -- Create the index if the table is empty
    IF NOT EXISTS (SELECT 1 FROM ${POSTGRES_SCHEMA}.usernames LIMIT 1) THEN
        CREATE INDEX IF NOT EXISTS idx_usernames_upsert
        ON ${POSTGRES_SCHEMA}.usernames (id, updated_at);
    END IF;
END $$;


CREATE INDEX IF NOT EXISTS usernames_fid ON ${POSTGRES_SCHEMA}.usernames (fid);
