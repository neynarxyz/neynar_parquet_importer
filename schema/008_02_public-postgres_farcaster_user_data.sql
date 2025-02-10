CREATE INDEX IF NOT EXISTS user_data_timestamp_not_deleted ON user_data ("timestamp") WHERE deleted_at IS NULL;
