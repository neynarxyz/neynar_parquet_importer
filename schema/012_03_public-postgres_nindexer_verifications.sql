CREATE INDEX IF NOT EXISTS verifications_timestamp_not_deleted ON verifications ("timestamp") WHERE deleted_at IS NULL;
