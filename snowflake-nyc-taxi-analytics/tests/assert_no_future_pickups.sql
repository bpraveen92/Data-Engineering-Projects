select
    trip_id,
    pickup_datetime,
    _source_month
from {{ ref('fct_trips') }}
where pickup_datetime > current_timestamp()
