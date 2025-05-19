CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.profile_with_addresses
(
    fid bigint NOT NULL PRIMARY KEY,
    fname text COLLATE pg_catalog."default",
    display_name text COLLATE pg_catalog."default",
    avatar_url text COLLATE pg_catalog."default",
    bio text COLLATE pg_catalog."default",
    verified_addresses JSONB NOT NULL,
    updated_at timestamp without time zone NOT NULL
);

DO $$
BEGIN
    -- Create the index if the table is empty
    IF NOT EXISTS (SELECT 1 FROM ${POSTGRES_SCHEMA}.profile_with_addresses LIMIT 1) THEN
        CREATE INDEX IF NOT EXISTS idx_profile_with_addresses_upsert
        ON profile_with_addresses (id, updated_at);
    END IF;
END $$;

