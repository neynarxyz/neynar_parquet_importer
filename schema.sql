CREATE TABLE IF NOT EXISTS public.parquet_import_tracking (
    id bigint PRIMARY KEY,
    file_key VARCHAR UNIQUE,
    file_hash VARCHAR,
    is_full BOOLEAN DEFAULT FALSE,
    is_empty BOOLEAN DEFAULT FALSE,
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- TODO: index on parquet_import_tracking.imported_at

-- TODO: all the below tables schemas were initially copied from replicator_v1
-- TODO: but parquet doesn't export the messages table. this complicates things. we can't use the upstream schema...

CREATE TABLE IF NOT EXISTS public.casts
(
    id bigint PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    "timestamp" timestamp without time zone NOT NULL,
    fid bigint NOT NULL,
    hash bytea NOT NULL,
    parent_hash bytea,
    parent_fid bigint,
    parent_url text COLLATE pg_catalog."default",
    text text COLLATE pg_catalog."default" NOT NULL,
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
    hash bytea NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    type text COLLATE pg_catalog."default",
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
    hash bytea NOT NULL,
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
    hash bytea,
    custody_address bytea,
    signer bytea NOT NULL,
    name text COLLATE pg_catalog."default",
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
    hash bytea NOT NULL,
    type smallint NOT NULL,
    value text COLLATE pg_catalog."default" NOT NULL,
    CONSTRAINT user_data_fid_type_unique UNIQUE (fid, type),
    CONSTRAINT user_data_hash_unique UNIQUE (hash)
);

CREATE TABLE IF NOT EXISTS public.verifications
(
    id bigint PRIMARY KEY,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    deleted_at timestamp without time zone,
    "timestamp" timestamp without time zone NOT NULL,
    fid bigint NOT NULL,
    hash bytea NOT NULL,
    claim jsonb NOT NULL,
    CONSTRAINT verifications_hash_unique UNIQUE (hash)
);

CREATE TABLE IF NOT EXISTS public.warpcast_power_users
(
    fid bigint NOT NULL,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    deleted_at timestamp without time zone,
    CONSTRAINT warpcast_power_users_pkey PRIMARY KEY (fid)
);
