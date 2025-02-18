CREATE INDEX IF NOT EXISTS channel_follows_timestamp_not_deleted ON ${POSTGRES_SCHEMA}.channel_follows ("timestamp") WHERE deleted_at IS NULL;
