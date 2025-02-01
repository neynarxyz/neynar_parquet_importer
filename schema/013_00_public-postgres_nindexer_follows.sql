CREATE TABLE IF NOT EXISTS follows
(
    id UUID PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    "timestamp" timestamp without time zone NOT NULL,
    fid bigint NOT NULL,
    target_fid bigint NOT NULL
);

DO $$
BEGIN
    -- Add new columns to follows if they don't already exist
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'follows' AND column_name = 'display_timestamp'
    ) THEN
        ALTER TABLE follows
        ADD COLUMN display_timestamp TIMESTAMP;
    END IF;
END $$;
