CREATE INDEX CONCURRENTLY IF NOT EXISTS storage_timestamp_not_deleted ON storage ("timestamp") WHERE deleted_at IS NULL;
