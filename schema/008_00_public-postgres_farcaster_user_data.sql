CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.user_data
(
    id bigint PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    "timestamp" timestamp without time zone NOT NULL,
    fid bigint NOT NULL,
    "hash" bytea NOT NULL UNIQUE,
    "type" smallint NOT NULL,
    "value" text COLLATE pg_catalog."default" NOT NULL
);

DO $$
BEGIN
    -- Check if the constraint exists
    IF EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_name = 'user_data'
          AND table_schema = '${POSTGRES_SCHEMA}'
          AND constraint_name = 'user_data_fid_type_unique'
          AND constraint_type = 'UNIQUE'
    ) THEN
        -- Drop the constraint
        ALTER TABLE user_data DROP CONSTRAINT user_data_fid_type_unique;
    END IF;
END $$;

-- TODO: add indexes to the tables as needed
