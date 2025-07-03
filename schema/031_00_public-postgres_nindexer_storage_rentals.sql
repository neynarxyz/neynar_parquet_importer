-- Table: ${POSTGRES_SCHEMA}.storage_rentals

-- DROP TABLE IF EXISTS ${POSTGRES_SCHEMA}.storage_rentals;

CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.storage_rentals
(
    id uuid NOT NULL PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    fid integer NOT NULL,
    units integer NOT NULL,
    expiry timestamp without time zone NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    transaction_hash bytea,
    log_index integer
);

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.storage_rentals_fid_expiry_idx;

CREATE INDEX IF NOT EXISTS storage_rentals_fid_expiry_idx
    ON ${POSTGRES_SCHEMA}.storage_rentals USING btree
    (fid ASC NULLS LAST, expiry DESC NULLS FIRST);
-- Index: storage_rentals_fid_idx

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.storage_rentals_fid_idx;

CREATE INDEX IF NOT EXISTS storage_rentals_fid_idx
    ON ${POSTGRES_SCHEMA}.storage_rentals USING btree
    (fid ASC NULLS LAST);
-- Index: storage_rentals_updated_at_idx

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.storage_rentals_updated_at_idx;

CREATE INDEX IF NOT EXISTS storage_rentals_updated_at_idx
    ON ${POSTGRES_SCHEMA}.storage_rentals USING btree
    (updated_at ASC NULLS LAST);

-- Trigger: update_timestamp

-- DROP TRIGGER IF EXISTS update_timestamp ON ${POSTGRES_SCHEMA}.storage_rentals;
