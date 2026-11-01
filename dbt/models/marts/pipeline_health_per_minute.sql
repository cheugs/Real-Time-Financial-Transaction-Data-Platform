-- Pipeline health per minute of PROCESSING time (when Spark wrote the data).

select
    date_trunc('minute', processed_at)                          as minute,
    count(*)                                                    as events_processed,
    round(avg(processing_latency_s)::numeric, 2)                as avg_processing_latency_s,
    round(max(processing_latency_s)::numeric, 2)                as max_processing_latency_s,
    round(avg(arrival_delay_s)::numeric, 2)                     as avg_arrival_delay_s,
    count(*) filter (where arrival_delay_s > 60)                as late_events,
    count(*) filter (where arrival_delay_s > 600)               as beyond_watermark_events,
    count(*) filter (where schema_version = 1)                  as schema_v1_events,
    count(*) filter (where schema_version = 2)                  as schema_v2_events
from {{ ref('stg_transactions') }}
group by 1
order by 1
