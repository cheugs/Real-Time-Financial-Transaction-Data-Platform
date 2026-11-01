-- Runs automatically the first time the Postgres container starts.
-- Layout:
--   public.*    raw + streaming tables written by Spark (and reference data)
--   analytics.* transformed tables built by dbt

-- ---------------------------------------------------------------- reference data
-- Fixed DEMO exchange rates (not live rates). Spark loads them at start-up to
-- convert every amount to EUR, so totals never mix currencies.
CREATE TABLE IF NOT EXISTS fx_rates (
    currency     TEXT PRIMARY KEY,
    rate_to_eur  NUMERIC(14, 8) NOT NULL CHECK (rate_to_eur > 0),
    note         TEXT
);
INSERT INTO fx_rates (currency, rate_to_eur, note) VALUES
    ('EUR', 1.0,        'base currency'),
    ('GBP', 1.17,       'demo rate'),
    ('USD', 0.92,       'demo rate'),
    ('NGN', 0.00058,    'demo rate'),
    ('XAF', 0.00152449, 'fixed peg: 1 EUR = 655.957 XAF'),
    ('AED', 0.25,       'demo rate')
ON CONFLICT (currency) DO NOTHING;

-- ---------------------------------------------------------------- raw data
-- Every valid event, exactly once (event_id is the primary key -> no duplicates).
CREATE TABLE IF NOT EXISTS raw_transactions (
    event_id         TEXT PRIMARY KEY,
    schema_version   INT NOT NULL,
    account_id       TEXT NOT NULL,
    merchant_id      TEXT,
    amount           NUMERIC(18, 2) NOT NULL CHECK (amount > 0),
    currency         TEXT NOT NULL REFERENCES fx_rates (currency),
    amount_eur       NUMERIC(14, 2) NOT NULL,
    country          TEXT,
    status           TEXT NOT NULL CHECK (status IN ('APPROVED', 'DECLINED', 'PENDING')),
    payment_method   TEXT,          -- added in schema v2 (NULL for v1 events)
    device_id        TEXT,          -- added in schema v2 (NULL for v1 events)
    event_time       TIMESTAMPTZ NOT NULL,   -- when the payment happened
    kafka_time       TIMESTAMPTZ NOT NULL,   -- when it reached Kafka
    processed_at     TIMESTAMPTZ NOT NULL,   -- when Spark wrote it here
    kafka_partition  INT NOT NULL,
    kafka_offset     BIGINT NOT NULL
);
-- Why these indexes:
--   event_time   : the dashboard and dbt filter/group by event-time ranges
--   account+time : the suspicious-activity rules group by account over time
--   processed_at : "latency in the last 5 minutes" KPI scans recent rows only
CREATE INDEX IF NOT EXISTS idx_raw_event_time   ON raw_transactions (event_time);
CREATE INDEX IF NOT EXISTS idx_raw_account_time ON raw_transactions (account_id, event_time);
CREATE INDEX IF NOT EXISTS idx_raw_processed_at ON raw_transactions (processed_at);

-- Messages that failed parsing or a data-quality rule (a "dead letter" table).
-- Key = Kafka position, so a re-processed batch cannot insert them twice.
CREATE TABLE IF NOT EXISTS bad_events (
    kafka_partition  INT,
    kafka_offset     BIGINT,
    event_id         TEXT,
    error_reason     TEXT NOT NULL,
    raw_value        TEXT,
    processed_at     TIMESTAMPTZ,
    PRIMARY KEY (kafka_partition, kafka_offset)
);

-- ---------------------------------------------------------------- streaming aggregates
-- Per-minute aggregates written by Spark (event-time windows), amounts in EUR.
CREATE TABLE IF NOT EXISTS txn_volume_per_minute (
    window_start         TIMESTAMPTZ PRIMARY KEY,
    window_end           TIMESTAMPTZ NOT NULL,
    txn_count            BIGINT NOT NULL,
    total_amount_eur     NUMERIC(16, 2),
    approved_count       BIGINT,
    declined_count       BIGINT,
    avg_arrival_delay_s  DOUBLE PRECISION,
    max_arrival_delay_s  DOUBLE PRECISION,
    late_event_count     BIGINT,
    updated_at           TIMESTAMPTZ DEFAULT now()
);

-- ---------------------------------------------------------------- monitoring
-- One row per Spark micro-batch, so we can SEE duplicates being skipped.
CREATE TABLE IF NOT EXISTS batch_log (
    query_name          TEXT,
    batch_id            BIGINT,
    rows_received       INT,
    rows_inserted       INT,
    duplicates_skipped  INT,
    bad_rows            INT,
    logged_at           TIMESTAMPTZ DEFAULT now(),
    PRIMARY KEY (query_name, batch_id)
);

-- Spark progress reports: throughput, batch duration and Kafka consumer lag.
-- run_id changes every time the job restarts -> restarts are visible.
CREATE TABLE IF NOT EXISTS stream_progress (
    query_name                 TEXT,
    run_id                     TEXT,
    batch_id                   BIGINT,
    batch_timestamp            TIMESTAMPTZ,
    num_input_rows             BIGINT,
    input_rows_per_second      DOUBLE PRECISION,
    processed_rows_per_second  DOUBLE PRECISION,
    batch_duration_ms          BIGINT,
    kafka_lag                  BIGINT,     -- messages in Kafka not read yet
    PRIMARY KEY (query_name, run_id, batch_id)
);
CREATE INDEX IF NOT EXISTS idx_progress_time ON stream_progress (batch_timestamp);

-- Start / restart / failure / replay events of the pipeline.
CREATE TABLE IF NOT EXISTS pipeline_events (
    id          SERIAL PRIMARY KEY,
    logged_at   TIMESTAMPTZ DEFAULT now(),
    event_type  TEXT NOT NULL,
    query_name  TEXT,
    run_id      TEXT,
    details     TEXT
);
