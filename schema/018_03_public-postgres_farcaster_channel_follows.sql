CREATE INDEX CONCURRENTLY IF NOT EXISTS channel_follows_timestamp_not_deleted ON channel_follows ("timestamp") WHERE deleted_at IS NULL;
