/*
  int_trips_enriched
  ------------------
  Intermediate model that enriches raw trip data with dimension attributes.

  Why ephemeral?
  --------------
  This model is materialized as ephemeral (configured in dbt_project.yml).
  Ephemeral models are NOT written to Snowflake — instead, dbt compiles this
  SQL as a CTE that gets inlined into every downstream model that calls
  ref('int_trips_enriched').

  This is the correct choice here because:
    - int_trips_enriched is only consumed by marts (fct_trips, mart_*)
    - It's a pure join/enrichment step with no independent analytical value
    - Skipping materialization reduces Snowflake storage and compute costs
    - The compiled SQL still benefits from Snowflake's query optimizer

  What this model does
  --------------------
  1. JOINs stg_yellow_trips with:
       - taxi_zones (pickup) → pickup_borough, pickup_zone, pickup_service_zone
       - taxi_zones (dropoff) → dropoff_borough, dropoff_zone
       - payment_types      → payment_type_name
       - vendor_names       → vendor_name
  2. Adds classify_time_of_day(hour) → time_of_day
  3. Adds is_airport_trip flag
  4. Adds tip_pct (tip as percentage of fare, safe_divide protected)
*/

with trips as (
    select * from {{ ref('stg_yellow_trips') }}
),

pickup_zones as (
    select
        zone_id,
        borough     as pickup_borough,
        zone_name   as pickup_zone,
        service_zone as pickup_service_zone
    from {{ ref('taxi_zones') }}
),

dropoff_zones as (
    select
        zone_id,
        borough     as dropoff_borough,
        zone_name   as dropoff_zone,
        service_zone as dropoff_service_zone
    from {{ ref('taxi_zones') }}
),

payments as (
    select
        payment_type_id,
        payment_type_name
    from {{ ref('payment_types') }}
),

vendors as (
    select
        vendor_id,
        vendor_name
    from {{ ref('vendor_names') }}
),

enriched as (
    select
        -- Core trip identifiers
        t.trip_id,
        t.vendor_id,
        v.vendor_name,

        -- Timestamps and derived time fields
        t.pickup_datetime,
        t.dropoff_datetime,
        t.pickup_date,
        t.trip_duration_minutes,
        hour(t.pickup_datetime)                             as pickup_hour,
        dayofweek(t.pickup_datetime)                        as pickup_day_of_week,
        -- Day name (1=Sunday in Snowflake's dayofweek)
        decode(dayofweek(t.pickup_datetime),
            1, 'Sunday', 2, 'Monday', 3, 'Tuesday',
            4, 'Wednesday', 5, 'Thursday', 6, 'Friday', 7, 'Saturday'
        )                                                   as pickup_day_name,
        {{ classify_time_of_day('hour(t.pickup_datetime)') }} as time_of_day,

        -- Trip characteristics
        t.passenger_count,
        t.trip_distance,
        t.rate_code_id,
        t.store_and_fwd_flag,

        -- Location with human-readable names (LEFT JOIN — unknown zones get NULLs)
        t.pickup_location_id,
        pu.pickup_borough,
        pu.pickup_zone,
        pu.pickup_service_zone,

        t.dropoff_location_id,
        do_.dropoff_borough,
        do_.dropoff_zone,
        do_.dropoff_service_zone,

        -- Airport trip flag: true when pickup OR dropoff is at one of the 3 NYC airports
        (
            pu.pickup_zone in ('JFK Airport', 'LaGuardia Airport', 'Newark Airport')
            or do_.dropoff_zone in ('JFK Airport', 'LaGuardia Airport', 'Newark Airport')
        )::boolean                                          as is_airport_trip,

        -- Payment with readable label
        t.payment_type,
        p.payment_type_name,

        -- Fare components
        t.fare_amount,
        t.extra,
        t.mta_tax,
        t.tip_amount,
        t.tolls_amount,
        t.improvement_surcharge,
        t.congestion_surcharge,
        t.airport_fee,
        t.total_amount,

        -- Derived: tip as a percentage of fare amount (0 for cash trips)
        {{ safe_divide('t.tip_amount', 't.fare_amount', fallback=0) }} as tip_pct,

        -- Metadata
        t._loaded_at,
        t._source_month

    from trips t
    left join pickup_zones  pu  on t.pickup_location_id  = pu.zone_id
    left join dropoff_zones do_ on t.dropoff_location_id = do_.zone_id
    left join payments      p   on t.payment_type        = p.payment_type_id
    left join vendors       v   on t.vendor_id           = v.vendor_id
)

select * from enriched
