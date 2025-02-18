CREATE INDEX IF NOT EXISTS storage_timestamp_not_deleted ON ${POSTGRES_SCHEMA}.storage ("timestamp") WHERE deleted_at IS NULL;
