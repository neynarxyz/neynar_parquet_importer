CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.account_verifications
(
    id bigint PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone,
    fid bigint NOT NULL,
    platform character varying(20) COLLATE pg_catalog."default",
    platform_id text COLLATE pg_catalog."default" NOT NULL,
    platform_username text COLLATE pg_catalog."default" NOT NULL,
    verified_at timestamp without time zone
);
