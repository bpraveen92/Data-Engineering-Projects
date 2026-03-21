{{ config(transient = true) }}

with zones as (
    select * from {{ ref('taxi_zones') }}
)

select
    zone_id,
    borough,
    zone_name,
    service_zone,
    (zone_name in ('JFK Airport', 'LaGuardia Airport', 'Newark Airport')) as is_airport,
    (service_zone = 'Yellow Zone')                                         as is_manhattan_yellow_zone,
    (service_zone = 'Boro Zone')                                           as is_boro_zone

from zones
