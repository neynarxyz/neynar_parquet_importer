CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.usernames
(
    id uuid NOT NULL PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone,
    username text COLLATE pg_catalog."default" NOT NULL,
    fid integer NOT NULL,
    custody_address bytea,
    proof_timestamp timestamp without time zone NOT NULL,
    type smallint NOT NULL
)