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
    id uuid PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone,
    fid bigint NOT NULL,
    channel_id text COLLATE pg_catalog."default" NOT NULL,
    member_at timestamp without time zone NOT NULL,
    "role" smallint NOT NULL
);

CREATE INDEX IF NOT EXISTS channel_members_channel_id_deleted_at_fid_undeleted_idx
    ON ${POSTGRES_SCHEMA}.channel_members USING btree
    (channel_id COLLATE pg_catalog."default" ASC NULLS LAST, deleted_at ASC NULLS LAST, fid ASC NULLS LAST)
    TABLESPACE pg_default
    WHERE deleted_at IS NULL;
-- Index: channel_members_channel_id_fid_undeleted_idx

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.channel_members_channel_id_fid_undeleted_idx;

CREATE INDEX IF NOT EXISTS channel_members_channel_id_fid_undeleted_idx
    ON ${POSTGRES_SCHEMA}.channel_members USING btree
    (channel_id COLLATE pg_catalog."default" ASC NULLS LAST, fid ASC NULLS LAST)
    TABLESPACE pg_default
    WHERE deleted_at IS NULL;
-- Index: channel_members_fid_channel_id_undeleted_idx

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.channel_members_fid_channel_id_undeleted_idx;

CREATE INDEX IF NOT EXISTS channel_members_fid_channel_id_undeleted_idx
    ON ${POSTGRES_SCHEMA}.channel_members USING btree
    (fid ASC NULLS LAST, channel_id COLLATE pg_catalog."default" ASC NULLS LAST)
    TABLESPACE pg_default
    WHERE deleted_at IS NULL;
-- Index: channel_members_fid_deleted_at_member_at_undeleted_idx

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.channel_members_fid_deleted_at_member_at_undeleted_idx;

CREATE INDEX IF NOT EXISTS channel_members_fid_deleted_at_member_at_undeleted_idx
    ON ${POSTGRES_SCHEMA}.channel_members USING btree
    (fid ASC NULLS LAST, deleted_at ASC NULLS LAST, member_at DESC NULLS FIRST)
    TABLESPACE pg_default
    WHERE deleted_at IS NULL;
-- Index: channel_members_updated_at_idx

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.channel_members_updated_at_idx;

CREATE INDEX IF NOT EXISTS channel_members_updated_at_idx
    ON ${POSTGRES_SCHEMA}.channel_members USING btree
    (updated_at ASC NULLS LAST);
