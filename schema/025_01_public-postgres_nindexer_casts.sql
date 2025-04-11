-- NOTE! This table name will conflict with the v2 casts table!

CREATE INDEX IF NOT EXISTS casts_hash ON ${POSTGRES_SCHEMA}.casts ("hash");
