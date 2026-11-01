-- Clean view on top of the raw table.
-- Schema evolution: v1 events have no payment_method, so we fill a default.

select
    event_id,
    schema_version,
    account_id,
    merchant_id,
    amount,
    currency,
    amount_eur,
    country,
    status,
    coalesce(payment_method, 'unknown') as payment_method,
    device_id,
    event_time,
    kafka_time,
    processed_at,
    -- how late the event arrived in Kafka
    extract(epoch from (kafka_time - event_time))   as arrival_delay_s,
    -- how long the pipeline took (Kafka -> Postgres)
    extract(epoch from (processed_at - kafka_time)) as processing_latency_s
from {{ source('raw', 'raw_transactions') }}
