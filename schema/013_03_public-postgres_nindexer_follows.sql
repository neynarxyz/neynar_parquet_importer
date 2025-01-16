CREATE INDEX CONCURRENTLY IF NOT EXISTS follows_timestamp_not_deleted ON follows ("timestamp") WHERE deleted_at IS NULL;
