-- Transaction distribution by status (APPROVED / DECLINED / PENDING) per minute.

select
    date_trunc('minute', event_time) as minute,
    status,
    count(*)                         as txn_count,
    sum(amount_eur)                  as amount_eur
from {{ ref('stg_transactions') }}
group by 1, 2
