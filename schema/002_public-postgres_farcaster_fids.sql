CREATE TABLE IF NOT EXISTS fids
(
    fid bigint NOT NULL,
    created_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at timestamp without time zone NOT NULL DEFAULT CURRENT_TIMESTAMP,
    custody_address bytea NOT NULL,
    registered_at timestamp with time zone,
    CONSTRAINT fids_pkey PRIMARY KEY (fid)
);

-- TODO: add indexes to the tables as needed
