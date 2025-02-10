CREATE INDEX IF NOT EXISTS channels_timestamp_not_deleted ON channels ("timestamp") WHERE deleted_at IS NULL;
