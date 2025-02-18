CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.neynar_user_scores
(
    id UUID PRIMARY KEY,
    fid bigint NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    score real NOT NULL
);

DO $$
BEGIN
    -- Check if the constraint exists
    IF EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_name = 'neynar_user_scores'
          AND table_schema = '${POSTGRES_SCHEMA}'
          AND constraint_name = 'neynar_user_scores_fid_key'
          AND constraint_type = 'UNIQUE'
    ) THEN
        -- Drop the constraint
        ALTER TABLE neynar_user_scores DROP CONSTRAINT neynar_user_scores_fid_key;
    END IF;
END $$;
