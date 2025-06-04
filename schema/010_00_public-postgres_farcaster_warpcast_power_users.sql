CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.warpcast_power_users
(
    fid bigint NOT NULL PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone
);

DO $$
BEGIN
    -- Create the index if the table is empty
    IF NOT EXISTS (SELECT 1 FROM ${POSTGRES_SCHEMA}.warpcast_power_users LIMIT 1) THEN
        CREATE INDEX IF NOT EXISTS idx_warpcast_power_users_upsert
        ON ${POSTGRES_SCHEMA}.warpcast_power_users (fid, updated_at);
    END IF;
END $$;

