CREATE INDEX IF NOT EXISTS channel_members_timestamp_not_deleted ON ${POSTGRES_SCHEMA}.channel_members ("timestamp") WHERE deleted_at IS NULL;
