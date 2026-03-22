# Architecture — NYC Taxi Analytics (dbt + Snowflake)

## Overview

This project implements an **ELT analytics engineering pipeline** using dbt and Snowflake on top
of the NYC TLC Yellow Taxi Trip Records dataset.  It is the sixth project in a portfolio spanning
batch ETL, realtime streaming, event-driven, and lakehouse architectures.

The key distinction from previous projects: **all transformation logic lives in dbt SQL models**,
not Python notebooks or Spark jobs.  The only Python is the one-time data load script.

---

## The ELT Pattern

```
┌────────────────────┐      ┌──────────────────────────┐      ┌──────────────────────────┐
│  TLC Cloudfront    │      │  Snowflake RAW Schema     │      │  dbt Transformation      │
│  (public Parquet)  │─────▶│  YELLOW_TRIPDATA          │─────▶│  Staging → Marts         │
│  ~8M rows/month    │  L   │  (raw, append-only)       │  T   │  (tested, documented)    │
└────────────────────┘      └──────────────────────────┘      └──────────────────────────┘
     E (Extract)                    L (Load)                          T (Transform)
scripts/download_data.py     scripts/load_to_snowflake.py          dbt run / dbt test
```

- **E**: `scripts/download_data.py` downloads monthly Parquet files from TLC's public URL
- **L**: `scripts/load_to_snowflake.py` reads the Parquet with pandas and appends to Snowflake
         RAW schema via `write_pandas` (PUT + COPY internally — fastest bulk load method)
- **T**: dbt transforms raw data through three layers: Staging → Intermediate → Marts

---

## Snowflake Object Layout

```
Database: NYC_TAXI
├── RAW                       ← loaded by scripts/load_to_snowflake.py
│   ├── YELLOW_TRIPDATA       ← 24M rows of raw trip data
│   ├── TAXI_ZONES            ← 265-row seed (dbt seed)
│   ├── PAYMENT_TYPES         ← 6-row seed
│   └── VENDOR_NAMES          ← 3-row seed
│
├── DEV_STAGING (dev) / STAGING (prod)
│   └── STG_YELLOW_TRIPS      ← view: renamed, cast, filtered, surrogate key
│
├── DEV_MARTS_CORE (dev) / MARTS_CORE (prod)
│   ├── DIM_TAXI_ZONES        ← dimension table: zone attributes + flags
│   ├── DIM_DATES             ← date spine: 2023-01-01 through 2024-12-31
│   └── FCT_TRIPS             ← incremental fact table (clustered by pickup_date)
│
├── DEV_MARTS_FINANCE (dev) / MARTS_FINANCE (prod)
│   ├── MART_REVENUE_BY_ZONE  ← monthly revenue per pickup zone
│   └── MART_HOURLY_DEMAND    ← trip demand by hour × weekday (168 rows)
│
└── SNAPSHOTS
    └── ZONE_ATTRIBUTES_SNAPSHOT  ← SCD Type 2 on taxi zone borough/service_zone
```

The `DEV_` prefix on dev schemas is produced by the `generate_schema_name` macro, which prevents
dev runs from overwriting production data when both targets share the same Snowflake account.

---

## dbt Model Layers

### Layer 1: Staging (`models/staging/`)
- **Materialization**: view (never persisted — always reads latest raw data)
- **Source**: `source('raw', 'yellow_tripdata')`
- **What it does**:
  - Renames TLC column names to clean snake_case
  - Casts every column to its correct data type
  - Derives `trip_duration_minutes` and `pickup_date`
  - Generates `trip_id` surrogate key via `dbt_utils.generate_surrogate_key`
  - Filters out phantom rows (null datetimes, dropoff ≤ pickup)
- **Tests**: `not_null`, `unique`, `accepted_values`, `relationships` (FK to seeds),
  `dbt_expectations.expect_column_values_to_be_between`, `dbt_expectations.expect_column_to_exist`

### Layer 2: Intermediate (`models/intermediate/`)
- **Materialization**: **ephemeral** — compiled as a CTE inside downstream models
  (no Snowflake table created; avoids storing transient join results)
- **What it does**:
  - JOINs staging trips with taxi_zones (pickup + dropoff), payment_types, vendor_names
  - Applies `classify_time_of_day` macro → `time_of_day` bucket
  - Derives `is_airport_trip`, `tip_pct`, `pickup_hour`, `pickup_day_name`

### Layer 3: Marts (`models/marts/`)
- **Materialization**: table (persisted; BI tools read directly)
- **core/**: Dimensional model (dim_taxi_zones, dim_dates, fct_trips)
- **finance/**: Aggregate analytics (mart_revenue_by_zone, mart_hourly_demand)

#### fct_trips — Incremental + Clustering
`fct_trips` uses `materialized='incremental'` with `unique_key='trip_id'`:
- First run: full table build
- Subsequent runs: only new rows (`pickup_date > max(pickup_date)` in table)
- Clustered by `pickup_date` → Snowflake micro-partition pruning on date filters

---

## Macros

| Macro | Purpose |
|---|---|
| `generate_schema_name` | Routes models to `DEV_*` schemas in dev, plain schemas in prod |
| `safe_divide(num, den)` | `IFF(den = 0, 0, num / den)` — prevents division-by-zero in mart aggregations |
| `classify_time_of_day(hour)` | Maps 0–23 → `morning_rush / midday / evening_rush / evening / overnight` |

---

## Seeds

Static lookup tables loaded by `dbt seed` into the RAW schema.
dbt seeds are the correct pattern for small, rarely-changing reference data.

| Seed | Rows | Purpose |
|---|---|---|
| `taxi_zones.csv` | 265 | TLC-defined NYC taxi zones with borough and service_zone |
| `payment_types.csv` | 6 | Payment method codes and human-readable names |
| `vendor_names.csv` | 3 | TPEP provider IDs and names |

---

## Snapshot — SCD Type 2 on Zone Attributes

`snapshots/zone_attributes_snapshot.sql` tracks changes to `borough` and `service_zone`
in the taxi_zones seed using dbt's `strategy='check'`:

- **No `updated_at` column needed** — dbt compares current vs. stored values directly
- When a zone attribute changes, the old row gets `dbt_valid_to = now()` and a new row
  is inserted with `dbt_valid_from = now()`, `dbt_valid_to = NULL` (current record)
- Use this to answer: "What borough was zone X in when trip Y occurred?"

This mirrors `APPLY CHANGES INTO ... stored_as_scd_type=2` from the Databricks ecommerce project,
but declaratively in plain SQL without any streaming infrastructure.

---

## Data Quality

| Layer | dbt Test Type | Behaviour on Failure |
|---|---|---|
| Staging | `not_null`, `unique`, `relationships` | Warn (data quality metric) |
| Staging | `dbt_expectations.expect_column_values_to_be_between` | Warn — keeps row, records in run results |
| Marts | `accepted_values`, `not_null` | Error — fails `dbt test` |
| Singular tests | Custom SQL assertions (`tests/`) | Error — any returned row = failure |
| Pytest | Python queries against live Snowflake | Error — assertion failures reported by pytest |

---

## Packages Used

| Package | Usage |
|---|---|
| `dbt-labs/dbt_utils` | `generate_surrogate_key` (trip_id), `date_spine` (dim_dates) |
| `calogica/dbt_expectations` | `expect_column_values_to_be_between`, `expect_column_to_exist`, `expect_column_values_to_be_of_type` |

---

## Lineage DAG

```
source('raw', 'yellow_tripdata')
    │
    ▼
stg_yellow_trips (view)
    │
    ├── [seeds: taxi_zones, payment_types, vendor_names]
    │
    ▼
int_trips_enriched (ephemeral — compiled inline)
    │
    ├──────────────────────────────────┐
    ▼                                  ▼
fct_trips (incremental table)    mart_hourly_demand (table)
    │
    ├──────────────────────────────────────┐
    ▼                                      ▼
mart_revenue_by_zone (table)         [dim_taxi_zones] ← taxi_zones seed
                                     [dim_dates]      ← dbt_utils.date_spine

[zone_attributes_snapshot] ← taxi_zones seed (SCD2 tracking changes)

fct_trips ─▶ nyc_taxi_revenue_dashboard (exposure)
mart_hourly_demand ─▶ nyc_taxi_demand_heatmap (exposure)
fct_trips + dim_taxi_zones ─▶ nyc_taxi_zone_analytics (exposure)
```

Run `make dbt-docs` to view the full interactive lineage DAG in your browser.
