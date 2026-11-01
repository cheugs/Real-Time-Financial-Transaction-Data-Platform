-- Invalid timestamps must be rejected: no stored event is more than
-- 5 minutes in the future (the same rule Spark applies).

select event_id, event_time, kafka_time
from {{ source('raw', 'raw_transactions') }}
where event_time > kafka_time + interval '5 minutes'
