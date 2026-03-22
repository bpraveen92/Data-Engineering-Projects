with source as (
    select * from {{ source('raw', 'yellow_tripdata') }}
),

renamed as (
    select
        {{ dbt_utils.generate_surrogate_key([
            'vendor_id',
            'pickup_datetime',
            'pickup_location_id'
        ]) }}                                                   as trip_id,

        cast(vendor_id as integer)                              as vendor_id,
        cast(pickup_datetime as timestamp_ntz)                  as pickup_datetime,
        cast(dropoff_datetime as timestamp_ntz)                 as dropoff_datetime,
        cast(pickup_datetime as date)                           as pickup_date,
        datediff('minute', pickup_datetime, dropoff_datetime)   as trip_duration_minutes,

        cast(passenger_count as integer)                        as passenger_count,
        cast(trip_distance as float)                            as trip_distance,
        cast(rate_code_id as integer)                           as rate_code_id,
        upper(trim(store_and_fwd_flag))                         as store_and_fwd_flag,

        cast(pickup_location_id as integer)                     as pickup_location_id,
        cast(dropoff_location_id as integer)                    as dropoff_location_id,

        cast(payment_type as integer)                           as payment_type,

        cast(fare_amount as float)                              as fare_amount,
        cast(extra as float)                                    as extra,
        cast(mta_tax as float)                                  as mta_tax,
        cast(tip_amount as float)                               as tip_amount,
        cast(tolls_amount as float)                             as tolls_amount,
        cast(improvement_surcharge as float)                    as improvement_surcharge,
        cast(total_amount as float)                             as total_amount,
        coalesce(cast(congestion_surcharge as float), 0.0)      as congestion_surcharge,
        coalesce(cast(airport_fee as float), 0.0)               as airport_fee,

        _loaded_at,
        _source_month

    from source
    where
        pickup_datetime is not null
        and dropoff_datetime is not null
        and dropoff_datetime > pickup_datetime
    qualify
        -- I found duplicate rows in the TLC source data, so I'm keeping the first-loaded copy
        row_number() over (
            partition by vendor_id, pickup_datetime, pickup_location_id
            order by _loaded_at
        ) = 1
)

select * from renamed
