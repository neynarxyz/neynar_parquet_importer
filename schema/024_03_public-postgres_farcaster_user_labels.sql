CREATE INDEX CONCURRENTLY IF NOT EXISTS user_labels_timestamp_not_deleted ON user_labels ("timestamp") WHERE deleted_at IS NULL;
