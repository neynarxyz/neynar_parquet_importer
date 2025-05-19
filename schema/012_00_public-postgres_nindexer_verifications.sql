-- TODO: this will conflict with the public-postgres "verifications" table
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
);

DO $$
BEGIN
    -- Create the index if the table is empty
    IF NOT EXISTS (SELECT 1 FROM ${POSTGRES_SCHEMA}.verifications LIMIT 1) THEN
        CREATE INDEX IF NOT EXISTS idx_verifications_upsert
        ON verifications (id, updated_at);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS verifications_address ON ${POSTGRES_SCHEMA}.verifications ("address");
CREATE INDEX IF NOT EXISTS verifications_fid ON ${POSTGRES_SCHEMA}.verifications (fid);
CREATE INDEX IF NOT EXISTS verifications_timestamp_not_deleted ON ${POSTGRES_SCHEMA}.verifications ("timestamp") WHERE deleted_at IS NULL;
