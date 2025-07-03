CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.signers
(
    id uuid NOT NULL PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone,
    "timestamp" timestamp without time zone NOT NULL,
    fid bigint NOT NULL,
    app_fid bigint NOT NULL,
    signer bytea NOT NULL
);

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.signers_fid_idx;

CREATE INDEX IF NOT EXISTS signers_fid_idx
    ON ${POSTGRES_SCHEMA}.signers USING btree
    (fid ASC NULLS LAST);
-- Index: signers_signer_fid_idx

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.signers_signer_fid_idx;

CREATE INDEX IF NOT EXISTS signers_signer_fid_idx
    ON ${POSTGRES_SCHEMA}.signers USING btree
    (signer ASC NULLS LAST, fid ASC NULLS LAST);
-- Index: signers_updated_at_idx

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.signers_updated_at_idx;

CREATE INDEX IF NOT EXISTS signers_updated_at_idx
    ON ${POSTGRES_SCHEMA}.signers USING btree
    (updated_at ASC NULLS LAST);
