CREATE TABLE IF NOT EXISTS storage
(
    id bigint PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    "timestamp" timestamp without time zone NOT NULL,
    fid bigint NOT NULL,
    units bigint NOT NULL,
    expiry timestamp without time zone NOT NULL
);

DO $$
BEGIN
    -- Check if the constraint exists
    IF EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_name = 'storage'
          AND constraint_name = 'unique_fid_units_expiry'
          AND constraint_type = 'UNIQUE'
    ) THEN
        -- Drop the constraint
        ALTER TABLE storage DROP CONSTRAINT unique_fid_units_expiry;
    END IF;
END $$;

-- TODO: add indexes to the tables as needed
