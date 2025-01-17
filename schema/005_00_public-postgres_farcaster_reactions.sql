CREATE TABLE IF NOT EXISTS reactions
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
          AND constraint_name = 'reactions_hash_unique'
          AND constraint_type = 'UNIQUE'
    ) THEN
        -- Drop the constraint
        ALTER TABLE reactions DROP CONSTRAINT reactions_hash_unique;
    END IF;
END $$;

-- TODO: add indexes to the tables as needed
