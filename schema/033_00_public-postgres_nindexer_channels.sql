CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.channels
(
    id uuid NOT NULL PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone,
    channel_id text COLLATE pg_catalog."default" NOT NULL,
    url text COLLATE pg_catalog."default" NOT NULL,
    description text COLLATE pg_catalog."default" NOT NULL,
    image_url text COLLATE pg_catalog."default" NOT NULL,
    lead_fid bigint NOT NULL,
    moderator_fids bigint[] NOT NULL,
    follower_count integer NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    name text COLLATE pg_catalog."default" NOT NULL,
    description_mentions bigint[] NOT NULL,
    description_mentions_positions integer[] NOT NULL,
    member_count integer NOT NULL,
    public_casting boolean NOT NULL,
    external_link_title text COLLATE pg_catalog."default",
    external_link_url text COLLATE pg_catalog."default",
    header_image_url text COLLATE pg_catalog."default",
    pinned_cast_hash bytea
);


-- Index: channels_channel_id_pattern_idx

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.channels_channel_id_pattern_idx;

CREATE INDEX IF NOT EXISTS channels_channel_id_pattern_idx
    ON ${POSTGRES_SCHEMA}.channels USING btree
    (channel_id COLLATE pg_catalog."default" varchar_pattern_ops ASC NULLS LAST);
-- Index: channels_dehydrated_cover_idx

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.channels_dehydrated_cover_idx;

CREATE INDEX IF NOT EXISTS channels_dehydrated_cover_idx
    ON ${POSTGRES_SCHEMA}.channels USING btree
    (channel_id COLLATE pg_catalog."default" ASC NULLS LAST)
    INCLUDE(name, image_url);
-- Index: channels_id_to_url_idx

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.channels_id_to_url_idx;

CREATE INDEX IF NOT EXISTS channels_id_to_url_idx
    ON ${POSTGRES_SCHEMA}.channels USING btree
    (channel_id COLLATE pg_catalog."default" ASC NULLS LAST)
    INCLUDE(url);
-- Index: channels_lead_fid_idx

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.channels_lead_fid_idx;

CREATE INDEX IF NOT EXISTS channels_lead_fid_idx
    ON ${POSTGRES_SCHEMA}.channels USING btree
    (lead_fid ASC NULLS LAST);
-- Index: channels_moderator_fids_idx

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.channels_moderator_fids_idx;

CREATE INDEX IF NOT EXISTS channels_moderator_fids_idx
    ON ${POSTGRES_SCHEMA}.channels USING gin
    (moderator_fids);
-- Index: channels_name_lower_pattern_idx

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.channels_name_lower_pattern_idx;

CREATE INDEX IF NOT EXISTS channels_name_lower_pattern_idx
    ON ${POSTGRES_SCHEMA}.channels USING btree
    (lower(name) COLLATE pg_catalog."default" varchar_pattern_ops ASC NULLS LAST);
-- Index: channels_updated_at_idx

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.channels_updated_at_idx;

CREATE INDEX IF NOT EXISTS channels_updated_at_idx
    ON ${POSTGRES_SCHEMA}.channels USING btree
    (updated_at ASC NULLS LAST);
-- Index: channels_url_to_id_idx

-- DROP INDEX IF EXISTS ${POSTGRES_SCHEMA}.channels_url_to_id_idx;

CREATE INDEX IF NOT EXISTS channels_url_to_id_idx
    ON ${POSTGRES_SCHEMA}.channels USING btree
    (url COLLATE pg_catalog."default" ASC NULLS LAST)
    INCLUDE(channel_id);
