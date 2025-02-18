CREATE INDEX IF NOT EXISTS follows_timestamp_not_deleted ON ${POSTGRES_SCHEMA}.follows ("timestamp") WHERE deleted_at IS NULL;
