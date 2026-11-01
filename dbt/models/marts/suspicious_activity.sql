-- SIMULATED suspicious-activity indicators.
-- These are simple rules for a demo, NOT a real fraud detection model.
-- Thresholds are dbt vars (see dbt_project.yml). Amounts are compared in EUR
-- so that e.g. 3000 XAF (about 5 EUR) is not flagged as a large payment.
--
--   high_velocity   : many payments from one account in a 5-minute bucket
--   large_amount    : a single payment above a EUR threshold
--   country_hopping : several different countries for one account in one hour

with txns as (
    select * from {{ ref('stg_transactions') }}
),

high_velocity as (
    select
        account_id,
        'high_velocity' as rule,
        date_bin('5 minutes', event_time, timestamptz '2000-01-01') as window_start,
        count(*)        as txn_count,
        sum(amount_eur) as total_amount_eur,
        count(*) || ' payments in 5 minutes (threshold {{ var("velocity_min_txns") }})' as details
    from txns
    group by 1, 2, 3
    having count(*) >= {{ var('velocity_min_txns') }}
),

large_amount as (
    select
        account_id,
        'large_amount'  as rule,
        event_time      as window_start,
        1               as txn_count,
        amount_eur      as total_amount_eur,
        'single payment of ' || amount || ' ' || currency || ' = ' || amount_eur || ' EUR' as details
    from txns
    where amount_eur >= {{ var('large_amount_eur') }}
),

country_hopping as (
    select
        account_id,
        'country_hopping' as rule,
        date_trunc('hour', event_time) as window_start,
        count(*)          as txn_count,
        sum(amount_eur)   as total_amount_eur,
        count(distinct country) || ' countries in 1 hour: ' || string_agg(distinct country, ', ') as details
    from txns
    group by 1, 2, 3
    having count(distinct country) >= {{ var('country_hop_min_countries') }}
)

select * from high_velocity
union all
select * from large_amount
union all
select * from country_hopping
