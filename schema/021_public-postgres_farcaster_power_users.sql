CREATE TABLE IF NOT EXISTS power_users
(
    fid bigint NOT NULL PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone,
    status character varying(20) COLLATE pg_catalog."default",
    seed_at timestamp without time zone,
    CONSTRAINT power_users_status_enum_check CHECK (status::text = ANY (ARRAY['pending'::character varying, 'power'::character varying, 'revoked'::character varying]::text[]))
);
