-- Schema evolution: every event has a version >= 1, and only v2+ events
-- may carry the v2 fields (payment_method, device_id).

select event_id, schema_version, payment_method, device_id
from {{ source('raw', 'raw_transactions') }}
where schema_version < 1
   or (schema_version = 1 and (payment_method is not null or device_id is not null))
