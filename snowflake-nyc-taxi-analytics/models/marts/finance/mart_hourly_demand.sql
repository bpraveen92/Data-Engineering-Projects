{{ config(transient = true) }}

with trips as (
    select * from {{ ref('fct_trips') }}
),

-- distinct date count per slot used to compute avg_trips_per_day below
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

        {{ safe_divide(
            'sum(case when t.is_airport_trip then 1 else 0 end)',
            'count(t.trip_id)',
            fallback=0
        ) }}                                                            as pct_airport_trips,

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
        {{ safe_divide('a.trip_count', 'dc.distinct_date_count') }} as avg_trips_per_day
    from aggregated a
    left join date_counts dc
        on a.pickup_hour = dc.pickup_hour
        and a.pickup_day_of_week = dc.pickup_day_of_week
)

select * from final
