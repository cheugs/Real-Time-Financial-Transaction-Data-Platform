-- Volume per currency: original amounts AND the EUR equivalent.
-- Original amounts are only summed inside one currency (never across currencies).

select
    t.currency,
    f.rate_to_eur,
    count(*)            as txn_count,
    sum(t.amount)       as total_amount_local,
    sum(t.amount_eur)   as total_amount_eur
from {{ ref('stg_transactions') }} t
join {{ source('raw', 'fx_rates') }} f using (currency)
group by 1, 2
