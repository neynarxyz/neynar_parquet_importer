CREATE INDEX IF NOT EXISTS signers_timestamp_not_deleted ON ${POSTGRES_SCHEMA}.signers ("timestamp") WHERE deleted_at IS NULL;
