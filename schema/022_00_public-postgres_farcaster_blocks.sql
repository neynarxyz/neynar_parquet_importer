CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.blocks
(
    id bigint NOT NULL PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone,
    blocker_fid bigint NOT NULL,
    blocked_fid bigint NOT NULL,
    updated_at timestamp with time zone
)