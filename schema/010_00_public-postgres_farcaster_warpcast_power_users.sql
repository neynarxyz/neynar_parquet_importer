CREATE TABLE IF NOT EXISTS warpcast_power_users
(
    fid bigint NOT NULL PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone
);
