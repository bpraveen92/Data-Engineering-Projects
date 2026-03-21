/*
  dim_taxi_zones
  --------------
  Dimension table for NYC TLC Taxi Zones.

  Source: taxi_zones seed (265 rows — all TLC-defined zones).
  This is a slowly-changing dimension; use the snapshot
  (zone_attributes_snapshot) to track borough or service_zone changes over time.

  Materialization: table (dims are always tables — fast FK lookups from fct_trips)
  transient: true — dims are rebuilt from seed on every run; no need for Snowflake
  Time Travel storage on a 265-row reference table.
*/

{{ config(transient = true) }}

with zones as (
    select * from {{ ref('taxi_zones') }}
)

select
    zone_id,
    borough,
    zone_name,
    service_zone,

    -- Convenience flags used in mart aggregations
    (zone_name in ('JFK Airport', 'LaGuardia Airport', 'Newark Airport')) as is_airport,

    -- Manhattan core = Yellow Zone (the traditional taxi service area)
    (service_zone = 'Yellow Zone') as is_manhattan_yellow_zone,

    -- Outer borough = Boro Zone
    (service_zone = 'Boro Zone') as is_boro_zone

from zones
