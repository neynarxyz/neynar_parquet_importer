CREATE TABLE IF NOT EXISTS channel_follows
(
    id bigint PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone,
    fid bigint NOT NULL,
    channel_id text COLLATE pg_catalog."default" NOT NULL,
    "timestamp" timestamp without time zone NOT NULL
);

DO $$
BEGIN
    -- Check if the constraint exists
    IF EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_name = 'channel_follows'
          AND constraint_name = 'channel_follows_fid_channel_id_unique'
          AND constraint_type = 'UNIQUE'
    ) THEN
        -- Drop the constraint
        ALTER TABLE channel_follows DROP CONSTRAINT channel_follows_fid_channel_id_unique;
    END IF;
END $$;
