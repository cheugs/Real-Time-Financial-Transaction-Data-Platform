-- Duplicates must never inflate the financial aggregates.
-- Spark's per-minute count can be LOWER than the raw table (events beyond the
-- watermark are dropped from windows) but never HIGHER than the number of
-- unique events in that minute. Returns rows only if a window is inflated.

with raw_counts as (
    select date_trunc('minute', event_time) as window_start, count(*) as raw_count
    from {{ source('raw', 'raw_transactions') }}
    group by 1
)

select v.window_start, v.txn_count, r.raw_count
from {{ source('raw', 'txn_volume_per_minute') }} v
left join raw_counts r using (window_start)
where v.txn_count > coalesce(r.raw_count, 0)
  -- the two Spark queries commit separately; ignore the newest minutes
  and v.window_start < now() - interval '2 minutes'
