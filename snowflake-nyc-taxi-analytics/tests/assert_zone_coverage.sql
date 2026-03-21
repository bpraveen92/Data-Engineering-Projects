select distinct
    f.pickup_location_id    as zone_id,
    'pickup'                as zone_type
from {{ ref('fct_trips') }} f
left join {{ ref('dim_taxi_zones') }} z on f.pickup_location_id = z.zone_id
where z.zone_id is null

union all

select distinct
    f.dropoff_location_id   as zone_id,
    'dropoff'               as zone_type
from {{ ref('fct_trips') }} f
left join {{ ref('dim_taxi_zones') }} z on f.dropoff_location_id = z.zone_id
where z.zone_id is null
