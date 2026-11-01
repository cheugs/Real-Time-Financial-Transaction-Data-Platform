-- Handy queries to show the "engineering proofs".
-- Run with:  docker compose exec -T postgres psql -U fintech -d fintech -f - < scripts/checks.sql

\echo '--- 1. Duplicate handling: total rows vs distinct event_ids (should be equal)'
select count(*) as rows, count(distinct event_id) as distinct_ids from raw_transactions;

\echo '--- 2. Duplicates skipped by the idempotent sink'
select sum(rows_received) as received, sum(rows_inserted) as inserted,
       sum(duplicates_skipped) as duplicates_skipped
from batch_log where query_name = 'raw_sink';

\echo '--- 3. Late events (arrived > 60 s after they happened)'
select count(*) filter (where kafka_time - event_time > interval '60 seconds') as late,
       count(*) filter (where kafka_time - event_time > interval '10 minutes') as beyond_watermark,
       count(*) as total
from raw_transactions;

\echo '--- 4. Event-time windows: raw count vs windowed count (difference = events dropped by the watermark)'
select (select count(*) from raw_transactions) as raw_events,
       (select sum(txn_count) from txn_volume_per_minute) as events_in_windows;

\echo '--- 5. Schema evolution: v1 vs v2 events'
select schema_version, count(*), count(payment_method) as with_payment_method
from raw_transactions group by 1 order by 1;

\echo '--- 6. Malformed messages (dead letter table)'
select count(*) as bad_events from bad_events;
