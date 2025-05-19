CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.follow_counts
(
    id UUID PRIMARY KEY,
    fid bigint NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    follower_count bigint NOT NULL DEFAULT 0,
    following_count bigint NOT NULL DEFAULT 0
);

DO $$
BEGIN
    -- Check if the constraint exists
    IF EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_name = 'follow_counts'
          AND table_schema = '${POSTGRES_SCHEMA}'
          AND constraint_name = 'follow_counts_fid_key'
          AND constraint_type = 'UNIQUE'
    ) THEN
        -- Drop the constraint
        ALTER TABLE follow_counts DROP CONSTRAINT follow_counts_fid_key;
    END IF;

    -- Add filtered_following_count to follow_counts if it doesn't already exist
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'follow_counts'
        AND table_schema = '${POSTGRES_SCHEMA}'
        AND column_name = 'filtered_following_count'
    ) THEN
        ALTER TABLE ${POSTGRES_SCHEMA}.follow_counts
        ADD COLUMN filtered_following_count bigint NULL;
    END IF;

    -- Add filtered_follower_count to follow_counts if it doesn't already exist
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'follow_counts'
        AND table_schema = '${POSTGRES_SCHEMA}'
        AND column_name = 'filtered_follower_count'
    ) THEN
        ALTER TABLE ${POSTGRES_SCHEMA}.follow_counts
        ADD COLUMN filtered_follower_count bigint NULL;
    END IF;

    -- Create the index if the table is empty
    IF NOT EXISTS (SELECT 1 FROM ${POSTGRES_SCHEMA}.follow_counts LIMIT 1) THEN
        CREATE INDEX IF NOT EXISTS idx_follow_counts_upsert
        ON follow_counts (id, updated_at);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS follow_counts_fid ON ${POSTGRES_SCHEMA}.follow_counts (fid);
