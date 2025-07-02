CREATE TABLE IF NOT EXISTS ${POSTGRES_SCHEMA}.tier_purchases
(
    id uuid NOT NULL PRIMARY KEY,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    fid bigint NOT NULL,
    tier_type integer NOT NULL,
    duration_in_days bigint NOT NULL,
    payer bytea NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    transaction_hash bytea NOT NULL,
    log_index integer NOT NULL
);