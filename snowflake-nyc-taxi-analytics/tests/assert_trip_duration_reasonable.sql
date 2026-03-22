-- I'm using a 300-min ceiling to catch meter-left-on phantom trips; severity is warn because TLC source has real outliers
{{ config(severity = 'warn') }}

select
    trip_id,
    pickup_datetime,
    dropoff_datetime,
    trip_duration_minutes,
    pickup_zone,
    dropoff_zone,
    _source_month
from {{ ref('fct_trips') }}
where trip_duration_minutes not between 1 and 300
