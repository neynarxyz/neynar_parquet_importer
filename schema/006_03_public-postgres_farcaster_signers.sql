CREATE INDEX IF NOT EXISTS signers_timestamp_not_deleted ON signers ("timestamp") WHERE deleted_at IS NULL;
