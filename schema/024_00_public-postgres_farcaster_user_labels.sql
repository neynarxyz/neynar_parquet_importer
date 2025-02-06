CREATE TABLE IF NOT EXISTS user_labels
(
    id bigint NOT NULL PRIMARY KEY,
    source text COLLATE pg_catalog."default" NOT NULL,
    provider_fid bigint NOT NULL,
    target_fid bigint NOT NULL,
    label_type text COLLATE pg_catalog."default" NOT NULL,
    label_value text COLLATE pg_catalog."default" NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone
)
