-- Optional: create a dedicated database first, then connect to it in psql.
-- CREATE DATABASE glamira_analytics;
-- \connect glamira_analytics;

CREATE TABLE IF NOT EXISTS dim_date (
    date_key TEXT PRIMARY KEY,
    date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    month_name TEXT NOT NULL,
    week_of_year INT NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    day_name TEXT NOT NULL,
    is_weekend BOOLEAN NOT NULL
);

CREATE TABLE IF NOT EXISTS dim_product (
    product_key TEXT PRIMARY KEY,
    product_id TEXT NOT NULL,
    option_id TEXT,
    option_label TEXT,
    valid_from DATE,
    valid_to DATE,
    is_current BOOLEAN NOT NULL DEFAULT TRUE
);

CREATE TABLE IF NOT EXISTS dim_location (
    location_key TEXT PRIMARY KEY,
    ip TEXT,
    country TEXT,
    country_code TEXT,
    region TEXT,
    city TEXT,
    postal_code TEXT,
    latitude DOUBLE PRECISION,
    longitude DOUBLE PRECISION,
    timezone TEXT,
    isp TEXT
);

CREATE TABLE IF NOT EXISTS dim_customer (
    customer_key TEXT PRIMARY KEY,
    email TEXT
);

ALTER TABLE IF EXISTS dim_customer
    ADD COLUMN IF NOT EXISTS email TEXT;

CREATE TABLE IF NOT EXISTS fact_log_event (
    date_key TEXT,
    product_key TEXT,
    event_key TEXT PRIMARY KEY,
    event_id TEXT,
    event_timestamp TIMESTAMP,
    ip TEXT,
    device_id TEXT,
    user_agent TEXT,
    browser TEXT,
    resolution TEXT,
    api_version TEXT,
    location_key TEXT,
    store_id TEXT,
    local_time TEXT,
    collection TEXT,
    current_url TEXT,
    referrer_url TEXT,
    show_recommendation BOOLEAN,
    event_count INT NOT NULL DEFAULT 1,
    ingestion_timestamp TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    customer_key TEXT,
    CONSTRAINT fk_fact_log_event_date
        FOREIGN KEY (date_key) REFERENCES dim_date (date_key),
    CONSTRAINT fk_fact_log_event_product
        FOREIGN KEY (product_key) REFERENCES dim_product (product_key),
    CONSTRAINT fk_fact_log_event_location
        FOREIGN KEY (location_key) REFERENCES dim_location (location_key),
    CONSTRAINT fk_fact_log_event_customer
        FOREIGN KEY (customer_key) REFERENCES dim_customer (customer_key),
    CONSTRAINT chk_fact_log_event_count
        CHECK (event_count >= 0)
);

ALTER TABLE IF EXISTS fact_log_event
    ADD COLUMN IF NOT EXISTS browser TEXT;

CREATE TABLE IF NOT EXISTS stream_batch_log (
    query_name TEXT NOT NULL,
    batch_id BIGINT NOT NULL,
    processed_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (query_name, batch_id)
);

CREATE UNIQUE INDEX IF NOT EXISTS ux_dim_date_date
    ON dim_date (date);

CREATE INDEX IF NOT EXISTS idx_dim_product_lookup
    ON dim_product (product_id, option_id, is_current);

CREATE INDEX IF NOT EXISTS idx_dim_location_ip
    ON dim_location (ip);

CREATE INDEX IF NOT EXISTS idx_fact_log_event_date_key
    ON fact_log_event (date_key);

CREATE INDEX IF NOT EXISTS idx_fact_log_event_product_key
    ON fact_log_event (product_key);

CREATE INDEX IF NOT EXISTS idx_fact_log_event_location_key
    ON fact_log_event (location_key);

CREATE INDEX IF NOT EXISTS idx_fact_log_event_customer_key
    ON fact_log_event (customer_key);

CREATE INDEX IF NOT EXISTS idx_dim_customer_email
    ON dim_customer (email);

CREATE INDEX IF NOT EXISTS idx_fact_log_event_event_timestamp
    ON fact_log_event (event_timestamp);

CREATE INDEX IF NOT EXISTS idx_fact_log_event_store_id
    ON fact_log_event (store_id);

CREATE INDEX IF NOT EXISTS idx_fact_log_event_collection
    ON fact_log_event (collection);

CREATE INDEX IF NOT EXISTS idx_fact_log_event_browser
    ON fact_log_event (browser);
