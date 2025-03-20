-- NOTE: this will conflict with the public-postgres "verifications" table. put it in a different schema
CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.verifications
(
    id UUID PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    "timestamp" timestamp without time zone NOT NULL,
    "address" bytea NOT NULL,
    fid bigint NOT NULL,
    protocol smallint NOT NULL
    is_primary boolean NOT NULL DEFAULT false,
);

DO $$
BEGIN
    -- Add new columns to verifications if they don't already exist
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'verifications'
        AND table_schema = '${POSTGRES_SCHEMA}'
        AND column_name = 'is_primary'
    ) THEN
        ALTER TABLE ${POSTGRES_SCHEMA}.verifications
        ADD COLUMN is_primary BOOLEAN NOT NULL DEFAULT FALSE;
    END IF;
END $$;
