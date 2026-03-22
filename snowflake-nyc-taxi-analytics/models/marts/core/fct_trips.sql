{{
    config(
        materialized         = 'incremental',
        unique_key           = 'trip_id',
        incremental_strategy = 'merge',
        on_schema_change     = 'append_new_columns',
        cluster_by           = ['pickup_date'],
        tags                 = ['core', 'daily']
    )
}}

with enriched as (
    select * from {{ ref('int_trips_enriched') }}

    {% if is_incremental() %}
        {% if var('start_date', none) is not none %}
            -- Backfill mode: I pass an explicit date window via --vars to reprocess a specific range
            where pickup_date between '{{ var("start_date") }}' and '{{ var("end_date") }}'
        {% else %}
            where pickup_date > (select max(pickup_date) from {{ this }})
        {% endif %}
    {% endif %}
)

select
    trip_id,
    vendor_id,
    pickup_location_id,
    dropoff_location_id,
    payment_type,

    pickup_datetime,
    dropoff_datetime,
    pickup_date,
    pickup_hour,
    pickup_day_of_week,
    pickup_day_name,
    time_of_day,

    passenger_count,
    trip_distance,
    trip_duration_minutes,
    rate_code_id,
    store_and_fwd_flag,

    is_airport_trip,

    fare_amount,
    extra,
    mta_tax,
    tip_amount,
    tolls_amount,
    improvement_surcharge,
    congestion_surcharge,
    airport_fee,
    total_amount,
    tip_pct,

    vendor_name,
    payment_type_name,
    pickup_borough,
    pickup_zone,
    pickup_service_zone,
    dropoff_borough,
    dropoff_zone,

    _loaded_at,
    _source_month

from enriched
