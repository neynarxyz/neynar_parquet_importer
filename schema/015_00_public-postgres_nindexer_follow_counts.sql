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
END $$;
