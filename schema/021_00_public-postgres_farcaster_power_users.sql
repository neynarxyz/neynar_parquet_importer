CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.power_users
(
    fid bigint NOT NULL PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone,
    status character varying(20) COLLATE pg_catalog."default",
    seed_at timestamp without time zone,
    CONSTRAINT power_users_status_enum_check CHECK (status::text = ANY (ARRAY['pending'::character varying, 'power'::character varying, 'revoked'::character varying]::text[]))
);

DO $$
BEGIN
    -- Create the index if the table is empty
    IF NOT EXISTS (SELECT 1 FROM ${POSTGRES_SCHEMA}.power_users LIMIT 1) THEN
        CREATE INDEX IF NOT EXISTS idx_power_users_upsert
        ON ${POSTGRES_SCHEMA}.power_users (fid, updated_at);
    END IF;
END $$;

