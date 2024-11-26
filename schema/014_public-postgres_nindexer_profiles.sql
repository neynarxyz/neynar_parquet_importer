CREATE TABLE IF NOT EXISTS profiles
(
    id UUID PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    fid bigint NOT NULL,
    bio text,
    pfp_url text,
    "url" text,
    username text,
    display_name text,
    "location" text,
    latitude real,
    longitude real
);
