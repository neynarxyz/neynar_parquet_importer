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

CREATE INDEX IF NOT EXISTS verifications_address ON ${POSTGRES_SCHEMA}.verifications ("address");
CREATE INDEX IF NOT EXISTS verifications_fid ON ${POSTGRES_SCHEMA}.verifications (fid);
CREATE INDEX IF NOT EXISTS verifications_timestamp_not_deleted ON ${POSTGRES_SCHEMA}.verifications ("timestamp") WHERE deleted_at IS NULL;
