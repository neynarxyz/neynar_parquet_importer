CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.signers
(
    id bigint PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    "timestamp" timestamp without time zone NOT NULL,
    fid bigint NOT NULL,
    "hash" bytea,
    custody_address bytea,
    signer bytea NOT NULL,
    "name" text COLLATE pg_catalog."default",
    app_fid bigint
);

DO $$
BEGIN
    -- Check if the constraint exists
    IF EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_name = 'signers'
          AND table_schema = '${POSTGRES_SCHEMA}'
          AND constraint_name = 'unique_timestamp_fid_signer'
          AND constraint_type = 'UNIQUE'
    ) THEN
        -- Drop the constraint
        ALTER TABLE ${POSTGRES_SCHEMA}.signers DROP CONSTRAINT unique_timestamp_fid_signer;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS signers_fid ON ${POSTGRES_SCHEMA}.signers (fid);
CREATE INDEX IF NOT EXISTS signers_signer ON ${POSTGRES_SCHEMA}.signers (signer);
CREATE INDEX IF NOT EXISTS signers_timestamp_not_deleted ON ${POSTGRES_SCHEMA}.signers ("timestamp") WHERE deleted_at IS NULL;
