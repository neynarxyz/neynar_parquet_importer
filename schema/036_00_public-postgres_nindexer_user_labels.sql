-- Table: ${POSTGRES_SCHEMA}.user_labels

-- DROP TABLE IF EXISTS ${POSTGRES_SCHEMA}.user_labels;

CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.user_labels
(
    id uuid PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone,
    source text COLLATE pg_catalog."default" NOT NULL,
    provider_fid bigint NOT NULL,
    target_fid bigint NOT NULL,
    label_type text COLLATE pg_catalog."default" NOT NULL,
    label_value text COLLATE pg_catalog."default" NOT NULL,
    "timestamp" timestamp without time zone NOT NULL
);


-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.user_labels_timestamp_idx;

CREATE INDEX IF NOT EXISTS user_labels_timestamp_idx
    ON ${POSTGRES_SCHEMA}.user_labels USING btree
    ("timestamp" ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True);
-- Index: user_labels_updated_at_idx

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.user_labels_updated_at_idx;

CREATE INDEX IF NOT EXISTS user_labels_updated_at_idx
    ON ${POSTGRES_SCHEMA}.user_labels USING btree
    (updated_at ASC NULLS LAST)
    WITH (fillfactor=100, deduplicate_items=True);
