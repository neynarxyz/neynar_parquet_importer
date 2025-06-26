CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.reactions
(
    id bigint PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    "timestamp" timestamp without time zone NOT NULL,
    reaction_type smallint NOT NULL,
    fid bigint NOT NULL,
    "hash" bytea NOT NULL,
    target_hash bytea,
    target_fid bigint,
    target_url text COLLATE pg_catalog."default"
);

DO $$
BEGIN
    -- Check if the constraint exists
    IF EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_name = 'reactions'
          AND table_schema = '${POSTGRES_SCHEMA}'
          AND constraint_name = 'reactions_hash_unique'
          AND constraint_type = 'UNIQUE'
    ) THEN
        -- Drop the constraint
        ALTER TABLE ${POSTGRES_SCHEMA}.reactions DROP CONSTRAINT reactions_hash_unique;
    END IF;
    -- add creator_app_fid and deleter_app_fid columns if they don't already exist
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'reactions'
        AND table_schema = '${POSTGRES_SCHEMA}'
        AND column_name = 'creator_app_fid'
    ) THEN
        ALTER TABLE ${POSTGRES_SCHEMA}.reactions
        ADD COLUMN creator_app_fid bigint;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'reactions'
        AND table_schema = '${POSTGRES_SCHEMA}'
        AND column_name = 'deleter_app_fid'
    ) THEN
        ALTER TABLE ${POSTGRES_SCHEMA}.reactions
        ADD COLUMN deleter_app_fid bigint;
    END IF;

    -- Create the index if the table is empty
    IF NOT EXISTS (SELECT 1 FROM ${POSTGRES_SCHEMA}.reactions LIMIT 1) THEN
        CREATE INDEX IF NOT EXISTS idx_reactions_upsert
        ON ${POSTGRES_SCHEMA}.reactions (id, updated_at);
    END IF;
END $$;

-- TODO: add indexes to the tables as needed
CREATE INDEX IF NOT EXISTS reactions_fid ON ${POSTGRES_SCHEMA}.reactions (fid);
CREATE INDEX IF NOT EXISTS reactions_hash ON ${POSTGRES_SCHEMA}.reactions (hash);
CREATE INDEX IF NOT EXISTS reactions_target_fid ON ${POSTGRES_SCHEMA}.reactions (target_fid);
CREATE INDEX IF NOT EXISTS reactions_target_hash ON ${POSTGRES_SCHEMA}.reactions (target_hash);
CREATE INDEX IF NOT EXISTS reactions_timestamp_not_deleted ON ${POSTGRES_SCHEMA}.reactions ("timestamp") WHERE deleted_at IS NULL;
