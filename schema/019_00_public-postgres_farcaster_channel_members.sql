-- -- TODO: permissions on this are being a headache. just drop the table by hand.
-- DO $$
-- BEGIN
--     -- Check if replicator_v1's old seq exists
--     IF EXISTS (
--         SELECT 1
--         FROM pg_class c
--         JOIN pg_namespace n ON c.relnamespace = n.oid
--         WHERE c.relkind = 'S'
--         AND n.nspname = '${POSTGRES_SCHEMA}'
--         AND c.relname = 'channel_members_id_seq'

--     ) THEN
--         -- Drop the whole table
--         DROP TABLE ${POSTGRES_SCHEMA}.channel_members;
--         -- Drop the sequence
--         DROP SEQUENCE ${POSTGRES_SCHEMA}.channel_members_id_seq;
--     END IF;
-- END $$;


CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.channel_members
(
    id bigint PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone,
    fid bigint NOT NULL,
    channel_id text COLLATE pg_catalog."default" NOT NULL,
    "timestamp" timestamp without time zone NOT NULL
);

DO $$
BEGIN
    -- Check if the constraint exists
    IF EXISTS (
        SELECT 1
        FROM information_schema.table_constraints
        WHERE table_name = 'channel_members'
          AND table_schema = '${POSTGRES_SCHEMA}'
          AND constraint_name = 'channel_members_fid_channel_id_unique'
          AND constraint_type = 'UNIQUE'
    ) THEN
        -- Drop the constraint
        ALTER TABLE ${POSTGRES_SCHEMA}.channel_members DROP CONSTRAINT channel_members_fid_channel_id_unique;
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS channel_members_timestamp_not_deleted ON ${POSTGRES_SCHEMA}.channel_members ("timestamp") WHERE deleted_at IS NULL;
CREATE INDEX IF NOT EXISTS channel_members_fid ON ${POSTGRES_SCHEMA}.channel_members (fid);
CREATE INDEX IF NOT EXISTS channel_members_channel_id ON ${POSTGRES_SCHEMA}.channel_members (channel_id);
