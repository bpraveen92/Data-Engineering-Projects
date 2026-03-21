/*
  fct_trips
  ---------
  Central fact table.  Grain: one row per taxi trip.

  Materialization: incremental table with unique_key=trip_id.
  Clustering key: pickup_date (configured in fct_trips.yml).

  Incremental strategy
  --------------------
  On the first run (cold start), dbt builds the full table.
  On subsequent runs, dbt only processes rows where pickup_date is newer than
  the maximum pickup_date already in the table.  This means loading a new
  month's data requires only: make load MONTH=YYYY-MM → make dbt-run.

  Backfill override
  -----------------
  When start_date and end_date variables are set (via `make backfill`), the
  WHERE clause is replaced to reprocess a specific window.  Use
  --full-refresh with backfill to drop and recreate the table.

  Model contract
  --------------
  fct_trips.yml declares contract: {enforced: true} with explicit data_type
  per column.  dbt will fail the run if the SELECT list doesn't match the
  declared schema.  This is a production safety net against silent column
  additions or renames breaking downstream BI.

  Cluster by pickup_date
  ----------------------
  Snowflake stores micro-partitions sorted by pickup_date, so queries that
  filter on a date range (e.g. "last 30 days") scan far fewer partitions.
  This is the Snowflake equivalent of Databricks Liquid Clustering in F1/ecommerce.
*/

{{
    config(
        materialized        = 'incremental',
        unique_key          = 'trip_id',
        incremental_strategy = 'merge',
        on_schema_change    = 'sync_all_columns',
        cluster_by          = ['pickup_date'],
        tags                = ['core', 'daily']
    )
}}

with enriched as (
    select * from {{ ref('int_trips_enriched') }}

    {% if is_incremental() %}

        {% if var('start_date', none) is not none %}
            -- Backfill mode: explicit date window passed via --vars
            where pickup_date between '{{ var("start_date") }}' and '{{ var("end_date") }}'
        {% else %}
            -- Normal incremental: only process dates newer than what's already in the table
            where pickup_date > (select max(pickup_date) from {{ this }})
        {% endif %}

    {% endif %}
)

select
    -- Keys
    trip_id,
    vendor_id,
    pickup_location_id,
    dropoff_location_id,
    payment_type,

    -- Timestamps and time dimensions
    pickup_datetime,
    dropoff_datetime,
    pickup_date,
    pickup_hour,
    pickup_day_of_week,
    pickup_day_name,
    time_of_day,

    -- Trip measures
    passenger_count,
    trip_distance,
    trip_duration_minutes,
    rate_code_id,
    store_and_fwd_flag,

    -- Flags
    is_airport_trip,

    -- Fare measures
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

    -- Enriched labels (denormalised for BI convenience)
    vendor_name,
    payment_type_name,
    pickup_borough,
    pickup_zone,
    pickup_service_zone,
    dropoff_borough,
    dropoff_zone,

    -- Metadata
    _loaded_at,
    _source_month

from enriched
