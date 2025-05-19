CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.blocks
(
    id bigint NOT NULL PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone,
    blocker_fid bigint NOT NULL,
    blocked_fid bigint NOT NULL,
    updated_at timestamp with time zone
);

DO $$
BEGIN
    -- Create the index if the table is empty
    IF NOT EXISTS (SELECT 1 FROM ${POSTGRES_SCHEMA}.blocks LIMIT 1) THEN
        CREATE INDEX IF NOT EXISTS idx_blocks_upsert
        ON blocks (id, updated_at);
    END IF;
END $$;


CREATE INDEX IF NOT EXISTS blocks_blocker_fid ON ${POSTGRES_SCHEMA}.blocks (blocker_fid);
CREATE INDEX IF NOT EXISTS blocks_blocked_fid ON ${POSTGRES_SCHEMA}.blocks (blocked_fid);
