CREATE INDEX CONCURRENTLY IF NOT EXISTS reactions_timestamp_not_deleted ON reactions ("timestamp") WHERE deleted_at IS NULL;
