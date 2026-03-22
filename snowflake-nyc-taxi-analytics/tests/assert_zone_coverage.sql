/*
  assert_zone_coverage
  --------------------
  Singular test: fails if any trip's pickup or dropoff zone ID does not exist
  in the dim_taxi_zones dimension table.

  This is a referential integrity check that the built-in `relationships` schema
  test also covers on stg_yellow_trips, but this test extends it to the fact
  table to catch any issues introduced between staging and marts.

  If new TLC zones are added by the city and the seed CSV is not updated,
  trips will contain zone IDs with no matching dimension row.  This silently
  produces NULLs in pickup_zone/pickup_borough on fct_trips, which in turn
  makes the revenue-by-zone mart incomplete.

  Returns zone IDs present in fct_trips but missing from dim_taxi_zones.
  Test passes when count = 0.
*/

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
