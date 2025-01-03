CREATE TABLE IF NOT EXISTS neynar_user_scores
(
    id UUID PRIMARY KEY,
    fid bigint NOT NULL UNIQUE,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    score real NOT NULL
);
