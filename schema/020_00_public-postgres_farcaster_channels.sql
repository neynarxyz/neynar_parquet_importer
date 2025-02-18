CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.channels
(
    id bigint PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone,
    channel_id text COLLATE pg_catalog."default" NOT NULL,
    url text COLLATE pg_catalog."default" NOT NULL,
    description text COLLATE pg_catalog."default" NOT NULL,
    image_url text COLLATE pg_catalog."default" NOT NULL,
    lead_fid bigint NOT NULL,
    moderator_fids bigint[] NOT NULL,
    follower_count integer NOT NULL,
    "timestamp" timestamp without time zone NOT NULL
);
