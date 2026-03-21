/*
  mart_hourly_demand
  ------------------
  Trip demand aggregated by hour-of-day and day-of-week.

  Grain: one row per (pickup_hour, pickup_day_of_week).
  This produces a 24 × 7 = 168-row table that is ideal for a heatmap
  visualization of NYC taxi demand patterns.

  Key metrics:
    - trip_count            : total trips across all dates in loaded data
    - avg_trip_distance     : average distance for that hour/weekday slot
    - avg_trip_duration     : average duration in minutes
    - avg_fare              : average metered fare
    - pct_airport_trips     : share of trips going to/from airports
    - pct_credit_card       : share of trips paid by credit card

  Note: trip_count here is the aggregate over all loaded months (Jan–Mar 2024).
  For per-day averages, divide by the number of distinct dates in the loaded data.

  transient: true — 168-row aggregate rebuilt on every run; Time Travel unnecessary.
*/

{{ config(transient = true) }}

with trips as (
    select * from {{ ref('fct_trips') }}
),

-- Count distinct loaded dates per hour/weekday — used to normalize trip counts
date_counts as (
    select
        pickup_hour,
        pickup_day_of_week,
        count(distinct pickup_date) as distinct_date_count
    from trips
    group by pickup_hour, pickup_day_of_week
),

aggregated as (
    select
        t.pickup_hour,
        t.pickup_day_of_week,
        t.pickup_day_name,
        t.time_of_day,

        count(t.trip_id)                                                as trip_count,
        avg(t.trip_distance)                                            as avg_trip_distance,
        avg(t.trip_duration_minutes)                                    as avg_trip_duration_minutes,
        avg(t.fare_amount)                                              as avg_fare,
        avg(t.total_amount)                                             as avg_total_amount,

        -- Airport share: what fraction of trips in this slot go to/from airports
        {{ safe_divide(
            'sum(case when t.is_airport_trip then 1 else 0 end)',
            'count(t.trip_id)',
            fallback=0
        ) }}                                                            as pct_airport_trips,

        -- Credit card payment share
        {{ safe_divide(
            'sum(case when t.payment_type = 1 then 1 else 0 end)',
            'count(t.trip_id)',
            fallback=0
        ) }}                                                            as pct_credit_card

    from trips t
    group by
        t.pickup_hour,
        t.pickup_day_of_week,
        t.pickup_day_name,
        t.time_of_day
),

final as (
    select
        a.*,
        dc.distinct_date_count,
        -- Average trips per day for this slot (useful for staffing models)
        {{ safe_divide('a.trip_count', 'dc.distinct_date_count') }} as avg_trips_per_day
    from aggregated a
    left join date_counts dc
        on a.pickup_hour = dc.pickup_hour
        and a.pickup_day_of_week = dc.pickup_day_of_week
)

select * from final
