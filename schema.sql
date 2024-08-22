DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM pg_type WHERE typname = 'file_type_enum') THEN
        CREATE TYPE file_type_enum AS ENUM ('full', 'incremental');
    END IF;
END $$;

CREATE TABLE IF NOT EXISTS public.parquet_import_tracking (
    id BIGSERIAL PRIMARY KEY,
    table_name VARCHAR NOT NULL,
    file_name VARCHAR UNIQUE,
    file_type file_type_enum,
    is_empty BOOLEAN,
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    last_row_group_imported INT DEFAULT NULL,
    total_row_groups INT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_parquet_import_tracking_table_name ON public.parquet_import_tracking(table_name);
CREATE INDEX IF NOT EXISTS idx_parquet_import_tracking_imported_at ON public.parquet_import_tracking(imported_at);

CREATE TABLE IF NOT EXISTS public.casts
(
    id bigint PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    "timestamp" timestamp without time zone NOT NULL,
    fid bigint NOT NULL,
    "hash" bytea NOT NULL,
    parent_hash bytea,
    parent_fid bigint,
    parent_url text COLLATE pg_catalog."default",
    "text" text COLLATE pg_catalog."default" NOT NULL,
    embeds jsonb NOT NULL DEFAULT '{}'::jsonb,
    mentions bigint[] NOT NULL DEFAULT '{}'::bigint[],
    mentions_positions smallint[] NOT NULL DEFAULT '{}'::smallint[],
    root_parent_hash bytea,
    root_parent_url text COLLATE pg_catalog."default",
    CONSTRAINT casts_hash_unique UNIQUE (hash)
);

CREATE TABLE IF NOT EXISTS public.fids
(
    fid bigint NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    custody_address bytea NOT NULL,
    registered_at timestamp with time zone,
    CONSTRAINT fids_pkey PRIMARY KEY (fid)
);

CREATE TABLE IF NOT EXISTS public.fnames
(
    fname text COLLATE pg_catalog."default" NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    custody_address bytea,
    expires_at timestamp without time zone,
    fid bigint,
    deleted_at timestamp without time zone,
    CONSTRAINT fnames_pkey PRIMARY KEY (fname)
);

CREATE TABLE IF NOT EXISTS public.links
(
    id bigint PRIMARY KEY,
    fid bigint,
    target_fid bigint,
    "hash" bytea NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    "type" text COLLATE pg_catalog."default",
    display_timestamp timestamp without time zone,
    CONSTRAINT links_fid_target_fid_type_unique UNIQUE (fid, target_fid, type),
    CONSTRAINT links_hash_unique UNIQUE (hash)
);


CREATE TABLE IF NOT EXISTS public.reactions
(
    id bigint PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    "timestamp" timestamp without time zone NOT NULL,
    reaction_type smallint NOT NULL,
    fid bigint NOT NULL,
    "hash" bytea NOT NULL,
    target_hash bytea,
    target_fid bigint,
    target_url text COLLATE pg_catalog."default",
    CONSTRAINT reactions_hash_unique UNIQUE (hash)
);

CREATE TABLE IF NOT EXISTS public.signers
(
    id bigint PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    "timestamp" timestamp without time zone NOT NULL,
    fid bigint NOT NULL,
    "hash" bytea,
    custody_address bytea,
    signer bytea NOT NULL,
    "name" text COLLATE pg_catalog."default",
    app_fid bigint,
    CONSTRAINT unique_timestamp_fid_signer UNIQUE ("timestamp", fid, signer)
);

CREATE TABLE IF NOT EXISTS public.storage
(
    id bigint PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    "timestamp" timestamp without time zone NOT NULL,
    fid bigint NOT NULL,
    units bigint NOT NULL,
    expiry timestamp without time zone NOT NULL,
    CONSTRAINT unique_fid_units_expiry UNIQUE (fid, units, expiry)
);

CREATE TABLE IF NOT EXISTS public.user_data
(
    id bigint PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    "timestamp" timestamp without time zone NOT NULL,
    fid bigint NOT NULL,
    "hash" bytea NOT NULL UNIQUE,
    "type" smallint NOT NULL,
    "value" text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT user_data_fid_type_unique UNIQUE (fid, type)
);

CREATE TABLE IF NOT EXISTS public.verifications
(
    id bigint PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    "timestamp" timestamp without time zone NOT NULL,
    fid bigint NOT NULL,
    "hash" bytea NOT NULL UNIQUE,
    claim jsonb NOT NULL
);

CREATE TABLE IF NOT EXISTS public.warpcast_power_users
(
    fid bigint NOT NULL PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone
);

CREATE TABLE IF NOT EXISTS public.profile_with_addresses
(
    fid bigint NOT NULL PRIMARY KEY,
    fname text COLLATE pg_catalog."default",
    display_name text COLLATE pg_catalog."default",
    avatar_url text COLLATE pg_catalog."default",
    bio text COLLATE pg_catalog."default",
    verified_addresses JSONB NOT NULL,
    updated_at timestamp without time zone NOT NULL
);

-- TODO: add indexes to the tables as needed
