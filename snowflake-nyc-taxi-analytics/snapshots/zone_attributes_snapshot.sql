/*
  zone_attributes_snapshot
  ------------------------
  SCD Type 2 snapshot tracking changes to taxi zone attributes over time.

  Why this snapshot exists
  ------------------------
  The taxi_zones seed is static CSV data, but in the real world TLC does
  occasionally reclassify zones — a neighbourhood might shift boroughs due
  to a boundary change, or a service_zone classification gets updated.

  If we simply updated the seed CSV, we'd lose the history of what zone
  a trip was in at the time it occurred.  This snapshot solves that by
  keeping all previous states with effective date ranges.

  How strategy='check' works
  --------------------------
  Unlike strategy='timestamp' (which requires an updated_at column), the
  'check' strategy compares the current seed values against the snapshot
  table row by row.  When dbt detects that borough or service_zone has
  changed for a zone_id:

    1. The old row gets dbt_valid_to set to the current timestamp
    2. A new row is inserted with dbt_valid_from = current timestamp
       and dbt_valid_to = NULL (marks the current/active record)

  dbt also adds these system columns automatically:
    - dbt_scd_id      : unique hash of (zone_id + dbt_valid_from) — PK of snapshot
    - dbt_valid_from  : when this version of the record became active
    - dbt_valid_to    : when this version was superseded (NULL = still current)
    - dbt_updated_at  : when the snapshot last touched this row

  To observe SCD2 in action:
    1. Run `make dbt-snapshot` → all 265 rows have dbt_valid_to = NULL
    2. Edit taxi_zones.csv: change one borough value
    3. Run `make dbt-seed` to reload the seed
    4. Run `make dbt-snapshot` again
    5. The old borough row will have dbt_valid_to set; a new row appears
       with the updated borough and dbt_valid_from = now

  Schema
  ------
  Written to the SNAPSHOTS schema in NYC_TAXI database (dev and prod share the
  same schema name here — snapshots are not environment-prefixed in most setups).
*/

{% snapshot zone_attributes_snapshot %}

  {{
    config(
      target_schema = 'snapshots',
      unique_key    = 'zone_id',
      strategy      = 'check',
      check_cols    = ['borough', 'service_zone']
    )
  }}

  select
      zone_id,
      borough,
      zone_name,
      service_zone
  from {{ ref('taxi_zones') }}

{% endsnapshot %}
