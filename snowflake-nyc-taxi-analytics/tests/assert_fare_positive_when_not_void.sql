-- I'm excluding payment types 3=no-charge, 4=dispute, 6=voided as they may legitimately have fare_amount <= 0
{{ config(severity = 'warn') }}

select
    trip_id,
    payment_type,
    payment_type_name,
    fare_amount,
    total_amount,
    _source_month
from {{ ref('fct_trips') }}
where
    payment_type not in (3, 4, 6)   -- exclude no-charge, dispute, voided
    and fare_amount <= 0
