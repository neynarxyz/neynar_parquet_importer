CREATE INDEX IF NOT EXISTS user_labels_timestamp_not_deleted ON ${POSTGRES_SCHEMA}.user_labels ("timestamp") WHERE deleted_at IS NULL;
