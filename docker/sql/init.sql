CREATE TABLE IF NOT EXISTS staging_dtt (
    acid TEXT,
    tran_id TEXT,
    tran_date TIMESTAMP,
    value_date TIMESTAMP,
    tran_amt NUMERIC,
    part_tran_type TEXT,
    tran_sub_type TEXT,
    tran_particulars TEXT,
    kafka_offset BIGINT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE TABLE IF NOT EXISTS pipeline_metadata (
    table_name TEXT PRIMARY KEY,
    last_value_date TIMESTAMP
);