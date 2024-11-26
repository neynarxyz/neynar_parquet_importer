CREATE TABLE IF NOT EXISTS profile_with_addresses
(
    fid bigint NOT NULL PRIMARY KEY,
    fname text COLLATE pg_catalog."default",
    display_name text COLLATE pg_catalog."default",
    avatar_url text COLLATE pg_catalog."default",
    bio text COLLATE pg_catalog."default",
    verified_addresses JSONB NOT NULL,
    updated_at timestamp without time zone NOT NULL
);

-- TODO: add indexes to the tables as needed
