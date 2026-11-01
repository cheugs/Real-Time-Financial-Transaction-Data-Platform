-- Data-quality rule: every stored transaction has a positive amount, a known
-- currency and an EUR amount that matches amount * rate (allowing rounding).
-- A dbt test passes when this query returns 0 rows.

select t.event_id, t.amount, t.currency, t.amount_eur, f.rate_to_eur
from {{ source('raw', 'raw_transactions') }} t
left join {{ source('raw', 'fx_rates') }} f using (currency)
where t.amount <= 0
   or f.currency is null
   or abs(t.amount * f.rate_to_eur - t.amount_eur) > 0.01
