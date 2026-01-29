CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.fids
(
    id uuid NOT NULL PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    fid integer NOT NULL,
    custody_address bytea NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    recovery_address bytea
);

DO $$
BEGIN
    -- Create the index if the table is empty
    -- add creator_app_fid and deleter_app_fid columns if they don't already exist
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'fids'
        AND table_schema = '${POSTGRES_SCHEMA}'
        AND column_name = 'registered_at'
    ) THEN
        ALTER TABLE ${POSTGRES_SCHEMA}.fids
        ADD COLUMN registered_at timestamp;
    END IF;


END $$;

CREATE INDEX IF NOT EXISTS fids_registered_at_idx
    ON ${POSTGRES_SCHEMA}.fids USING btree
    (registered_at ASC NULLS LAST);
-- Index: fids_registered_at_idx

CREATE INDEX IF NOT EXISTS fids_custody_address_idx
    ON ${POSTGRES_SCHEMA}.fids USING btree
    (custody_address ASC NULLS LAST);
-- Index: fids_custody_address_to_fid_idx

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.fids_custody_address_to_fid_idx;

CREATE INDEX IF NOT EXISTS fids_custody_address_to_fid_idx
    ON ${POSTGRES_SCHEMA}.fids USING btree
    (custody_address ASC NULLS LAST)
    INCLUDE(fid);
-- Index: fids_fid_idx

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.fids_fid_idx;

CREATE INDEX IF NOT EXISTS fids_fid_idx
    ON ${POSTGRES_SCHEMA}.fids USING btree
    (fid ASC NULLS LAST);
-- Index: fids_fid_to_custody_address_idx

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.fids_fid_to_custody_address_idx;

CREATE INDEX IF NOT EXISTS fids_fid_to_custody_address_idx
    ON ${POSTGRES_SCHEMA}.fids USING btree
    (fid ASC NULLS LAST)
    INCLUDE(custody_address);
-- Index: fids_timestamp_idx

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.fids_timestamp_idx;

CREATE INDEX IF NOT EXISTS fids_timestamp_idx
    ON ${POSTGRES_SCHEMA}.fids USING btree
    ("timestamp" DESC NULLS FIRST);
-- Index: fids_updated_at_idx

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.fids_updated_at_idx;

CREATE INDEX IF NOT EXISTS fids_updated_at_idx
    ON ${POSTGRES_SCHEMA}.fids USING btree
    (updated_at ASC NULLS LAST);
