CREATE INDEX CONCURRENTLY IF NOT EXISTS channel_members_timestamp_not_deleted ON channel_members ("timestamp") WHERE deleted_at IS NULL;
