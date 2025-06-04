CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.fids
(
    fid bigint NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    custody_address bytea NOT NULL,
    registered_at timestamp with time zone,
    CONSTRAINT fids_pkey PRIMARY KEY (fid)
);

DO $$
BEGIN
    -- Create the index if the table is empty
    IF NOT EXISTS (SELECT 1 FROM ${POSTGRES_SCHEMA}.fids LIMIT 1) THEN
        CREATE INDEX IF NOT EXISTS idx_fids_upsert
        ON ${POSTGRES_SCHEMA}.fids (fid, updated_at);
    END IF;
END $$;
