/*
  stg_yellow_trips
  ----------------
  Staging model for NYC TLC Yellow Taxi raw data.

  Responsibilities:
    - Rename source columns to consistent snake_case
    - Cast columns to correct data types
    - Derive trip_duration_minutes and pickup_date
    - Generate a stable surrogate key (trip_id) using dbt_utils
    - Apply dbt_expectations tests (see stg_yellow_trips.yml)

  Materialization: view (staging layer is always a view — never persisted)
  Source: {{ source('raw', 'yellow_tripdata') }}
*/

with source as (
    select * from {{ source('raw', 'yellow_tripdata') }}
),

renamed as (
    select
        -- Surrogate key: uniquely identifies a trip even without a natural PK.
        -- Combining vendor + pickup time + pickup zone gives near-perfect uniqueness.
        {{ dbt_utils.generate_surrogate_key([
            'vendor_id',
            'pickup_datetime',
            'pickup_location_id'
        ]) }}                                                   as trip_id,

        -- Vendor
        cast(vendor_id as integer)                              as vendor_id,

        -- Timestamps
        cast(pickup_datetime as timestamp_ntz)                  as pickup_datetime,
        cast(dropoff_datetime as timestamp_ntz)                 as dropoff_datetime,

        -- Derived date column — used as clustering key in fct_trips
        cast(pickup_datetime as date)                           as pickup_date,

        -- Derived duration — core metric for delivery SLA analysis
        datediff('minute', pickup_datetime, dropoff_datetime)   as trip_duration_minutes,

        -- Trip attributes
        cast(passenger_count as integer)                        as passenger_count,
        cast(trip_distance as float)                            as trip_distance,
        cast(rate_code_id as integer)                           as rate_code_id,
        upper(trim(store_and_fwd_flag))                         as store_and_fwd_flag,

        -- Location IDs (FK to taxi_zones seed)
        cast(pickup_location_id as integer)                     as pickup_location_id,
        cast(dropoff_location_id as integer)                    as dropoff_location_id,

        -- Payment
        cast(payment_type as integer)                           as payment_type,

        -- Fare components
        cast(fare_amount as float)                              as fare_amount,
        cast(extra as float)                                    as extra,
        cast(mta_tax as float)                                  as mta_tax,
        cast(tip_amount as float)                               as tip_amount,
        cast(tolls_amount as float)                             as tolls_amount,
        cast(improvement_surcharge as float)                    as improvement_surcharge,
        cast(total_amount as float)                             as total_amount,
        coalesce(cast(congestion_surcharge as float), 0.0)      as congestion_surcharge,
        coalesce(cast(airport_fee as float), 0.0)               as airport_fee,

        -- Metadata
        _loaded_at,
        _source_month

    from source
    where
        -- Filter out obvious phantom rows: trips with no pickup or dropoff time
        pickup_datetime is not null
        and dropoff_datetime is not null
        -- Drop rows where dropoff precedes pickup (data error)
        and dropoff_datetime > pickup_datetime
)

select * from renamed
