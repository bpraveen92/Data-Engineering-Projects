{{ config(transient = true) }}

with trips as (
    select * from {{ ref('fct_trips') }}
),

dates as (
    select date_day, year_month from {{ ref('dim_dates') }}
),

by_zone_month as (
    select
        t.pickup_location_id,
        t.pickup_zone,
        t.pickup_borough,
        t.pickup_service_zone,
        d.year_month,

        count(t.trip_id)                                                        as trip_count,
        sum(t.fare_amount)                                                      as total_fare,
        sum(t.total_amount)                                                     as total_revenue,
        sum(t.tip_amount)                                                       as total_tips,
        avg(t.trip_distance)                                                    as avg_trip_distance,
        avg(t.trip_duration_minutes)                                            as avg_trip_duration_minutes,
        {{ safe_divide('sum(t.fare_amount)', 'count(t.trip_id)') }}             as avg_fare_per_trip,
        {{ safe_divide('avg(t.tip_pct)', '1', fallback=0) }}                    as avg_tip_pct,
        sum(case when t.is_airport_trip then 1 else 0 end)                      as airport_trip_count,
        sum(case when t.payment_type = 1 then 1 else 0 end)                     as credit_card_trip_count,
        sum(case when t.payment_type = 2 then 1 else 0 end)                     as cash_trip_count

    from trips t
    inner join dates d on t.pickup_date = d.date_day
    group by
        t.pickup_location_id,
        t.pickup_zone,
        t.pickup_borough,
        t.pickup_service_zone,
        d.year_month
)

select * from by_zone_month
