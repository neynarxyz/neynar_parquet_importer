CREATE TABLE IF NOT EXISTS storage
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

-- TODO: add indexes to the tables as needed
