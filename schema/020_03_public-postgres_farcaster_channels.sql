CREATE INDEX IF NOT EXISTS channels_timestamp_not_deleted ON ${POSTGRES_SCHEMA}.channels ("timestamp") WHERE deleted_at IS NULL;
