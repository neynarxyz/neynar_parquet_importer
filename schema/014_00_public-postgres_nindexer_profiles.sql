CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.profiles
(
    id UUID PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    fid bigint NOT NULL,
    bio text,
    pfp_url text,
    "url" text,
    username text,
    display_name text,
    "location" text,
    latitude real,
    longitude real,
    primary_eth_address bytea,
    primary_sol_address bytea
);

DO $$
BEGIN
    -- Add new columns to profiles if they don't already exist
    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'profiles'
        AND table_schema = '${POSTGRES_SCHEMA}'
        AND column_name = 'primary_eth_address'
    ) THEN
        ALTER TABLE ${POSTGRES_SCHEMA}.profiles
        ADD COLUMN primary_eth_address bytea;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'profiles'
        AND table_schema = '${POSTGRES_SCHEMA}'
        AND column_name = 'primary_sol_address'
    ) THEN
        ALTER TABLE ${POSTGRES_SCHEMA}.profiles
        ADD COLUMN primary_sol_address bytea;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'profiles'
        AND table_schema = '${POSTGRES_SCHEMA}'
        AND column_name = 'banner_url'
    ) THEN
        ALTER TABLE ${POSTGRES_SCHEMA}.profiles
        ADD COLUMN banner_url text;
    END IF;

    IF NOT EXISTS (
        SELECT 1
        FROM information_schema.columns
        WHERE table_name = 'profiles'
        AND table_schema = '${POSTGRES_SCHEMA}'
        AND column_name = 'token_uri'
    ) THEN
        ALTER TABLE ${POSTGRES_SCHEMA}.profiles
        ADD COLUMN token_uri text;
    END IF;

    -- Create the index if the table is empty
    IF NOT EXISTS (SELECT 1 FROM ${POSTGRES_SCHEMA}.profiles LIMIT 1) THEN
        CREATE INDEX IF NOT EXISTS idx_profiles_upsert
        ON ${POSTGRES_SCHEMA}.profiles (id, updated_at);
    END IF;
END $$;

CREATE INDEX IF NOT EXISTS profiles_fid ON ${POSTGRES_SCHEMA}.profiles (fid);
