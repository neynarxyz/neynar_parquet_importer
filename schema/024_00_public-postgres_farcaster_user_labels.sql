CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.user_labels
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
);

DO $$
BEGIN
    -- Create the index if the table is empty
    IF NOT EXISTS (SELECT 1 FROM ${POSTGRES_SCHEMA}.user_labels LIMIT 1) THEN
        CREATE INDEX IF NOT EXISTS idx_user_labels_upsert
        ON ${POSTGRES_SCHEMA}.user_labels (id, updated_at);
    END IF;
END $$;


CREATE INDEX IF NOT EXISTS user_labels_provider_fid ON ${POSTGRES_SCHEMA}.user_labels (provider_fid);
CREATE INDEX IF NOT EXISTS user_labels_target_fid ON ${POSTGRES_SCHEMA}.user_labels (target_fid);
CREATE INDEX IF NOT EXISTS user_labels_timestamp_not_deleted ON ${POSTGRES_SCHEMA}.user_labels ("timestamp") WHERE deleted_at IS NULL;
