-- Table: ${POSTGRES_SCHEMA}.blocks

-- DROP TABLE IF EXISTS ${POSTGRES_SCHEMA}.blocks;

CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.blocks
(
    id uuid PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone,
    blocker_fid bigint NOT NULL,
    blocked_fid bigint NOT NULL
);

-- Index: blocks_blocked_fid_deleted_at

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.blocks_blocked_fid_deleted_at;

CREATE INDEX IF NOT EXISTS blocks_blocked_fid_deleted_at
    ON ${POSTGRES_SCHEMA}.blocks USING btree
    (blocked_fid ASC NULLS LAST, deleted_at ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True)
    WHERE deleted_at IS NULL;
-- Index: blocks_blocker_fid_deleted_at

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.blocks_blocker_fid_deleted_at;

CREATE INDEX IF NOT EXISTS blocks_blocker_fid_deleted_at
    ON ${POSTGRES_SCHEMA}.blocks USING btree
    (blocker_fid ASC NULLS LAST, deleted_at ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True)
    WHERE deleted_at IS NULL;
-- Index: blocks_updated_at_idx

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.blocks_updated_at_idx;

CREATE INDEX IF NOT EXISTS blocks_updated_at_idx
    ON ${POSTGRES_SCHEMA}.blocks USING btree
    (updated_at ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True);