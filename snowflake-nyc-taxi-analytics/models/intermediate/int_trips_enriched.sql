with trips as (
    select * from {{ ref('stg_yellow_trips') }}
),

pickup_zones as (
    select
        zone_id,
        borough      as pickup_borough,
        zone_name    as pickup_zone,
        service_zone as pickup_service_zone
    from {{ ref('taxi_zones') }}
),

dropoff_zones as (
    select
        zone_id,
        borough      as dropoff_borough,
        zone_name    as dropoff_zone,
        service_zone as dropoff_service_zone
    from {{ ref('taxi_zones') }}
),

payments as (
    select payment_type_id, payment_type_name
    from {{ ref('payment_types') }}
),

vendors as (
    select vendor_id, vendor_name
    from {{ ref('vendor_names') }}
),

enriched as (
    select
        t.trip_id,
        t.vendor_id,
        v.vendor_name,

        t.pickup_datetime,
        t.dropoff_datetime,
        t.pickup_date,
        t.trip_duration_minutes,
        hour(t.pickup_datetime)                             as pickup_hour,
        -- Snowflake DAYOFWEEK returns 0=Sunday…6=Saturday; +1 shifts to 1=Sunday…7=Saturday
        dayofweek(t.pickup_datetime) + 1                    as pickup_day_of_week,
        decode(dayofweek(t.pickup_datetime) + 1,
            1, 'Sunday', 2, 'Monday', 3, 'Tuesday',
            4, 'Wednesday', 5, 'Thursday', 6, 'Friday', 7, 'Saturday'
        )                                                   as pickup_day_name,
        {{ classify_time_of_day('hour(t.pickup_datetime)') }} as time_of_day,

        t.passenger_count,
        t.trip_distance,
        t.rate_code_id,
        t.store_and_fwd_flag,

        t.pickup_location_id,
        pu.pickup_borough,
        pu.pickup_zone,
        pu.pickup_service_zone,

        t.dropoff_location_id,
        do_.dropoff_borough,
        do_.dropoff_zone,
        do_.dropoff_service_zone,

        (
            pu.pickup_zone in ('JFK Airport', 'LaGuardia Airport', 'Newark Airport')
            or do_.dropoff_zone in ('JFK Airport', 'LaGuardia Airport', 'Newark Airport')
        )::boolean                                          as is_airport_trip,

        t.payment_type,
        p.payment_type_name,

        t.fare_amount,
        t.extra,
        t.mta_tax,
        t.tip_amount,
        t.tolls_amount,
        t.improvement_surcharge,
        t.congestion_surcharge,
        t.airport_fee,
        t.total_amount,
        {{ safe_divide('t.tip_amount', 't.fare_amount', fallback=0) }} as tip_pct,

        t._loaded_at,
        t._source_month

    from trips t
    left join pickup_zones  pu  on t.pickup_location_id  = pu.zone_id
    left join dropoff_zones do_ on t.dropoff_location_id = do_.zone_id
    left join payments      p   on t.payment_type        = p.payment_type_id
    left join vendors       v   on t.vendor_id           = v.vendor_id
)

select * from enriched
