CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.profile_external_accounts
(
    id uuid PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone,
    "timestamp" timestamp without time zone NOT NULL,
    fid bigint NOT NULL,
    app_fid bigint NOT NULL,
    type smallint NOT NULL,
    account text COLLATE pg_catalog."default" NOT NULL
);

-- Index: profile_external_accounts_account_type_idx

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.profile_external_accounts_account_type_idx;

CREATE INDEX IF NOT EXISTS profile_external_accounts_account_type_idx
    ON ${POSTGRES_SCHEMA}.profile_external_accounts USING btree
    (account COLLATE pg_catalog."default" ASC NULLS LAST, type ASC NULLS LAST)
    TABLESPACE pg_default
    WHERE deleted_at IS NULL;
-- Index: profile_external_accounts_fid_index

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.profile_external_accounts_fid_index;

CREATE INDEX IF NOT EXISTS profile_external_accounts_fid_index
    ON ${POSTGRES_SCHEMA}.profile_external_accounts USING btree
    (fid ASC NULLS LAST)
    TABLESPACE pg_default
    WHERE deleted_at IS NULL;
-- Index: profile_external_accounts_type_account_index

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.profile_external_accounts_type_account_index;

CREATE INDEX IF NOT EXISTS profile_external_accounts_type_account_index
    ON ${POSTGRES_SCHEMA}.profile_external_accounts USING btree
    (type ASC NULLS LAST, account COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default
    WHERE deleted_at IS NULL;
-- Index: profile_external_accounts_updated_at_idx

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.profile_external_accounts_updated_at_idx;

CREATE INDEX IF NOT EXISTS profile_external_accounts_updated_at_idx
    ON ${POSTGRES_SCHEMA}.profile_external_accounts USING btree
    (updated_at ASC NULLS LAST);
