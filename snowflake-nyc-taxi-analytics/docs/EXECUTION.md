# Execution Guide — NYC Taxi Analytics (dbt + Snowflake)

This guide walks through setting up and running the pipeline from scratch. If you want to understand how everything fits together first, read [ARCHITECTURE.md](ARCHITECTURE.md).

---

## Prerequisites

- Python 3.10+
- `uv` installed (`pip install uv` or `brew install uv`)
- A Snowflake account — the free 30-day trial at snowflake.com works, no credit card required
- `dbt` CLI — installed automatically as part of `make install`

---

## Step 0: Snowflake Setup

Log into your Snowflake account and run the following in a worksheet to create the database and schemas:

```sql
CREATE DATABASE IF NOT EXISTS NYC_TAXI;
CREATE SCHEMA IF NOT EXISTS NYC_TAXI.RAW;
CREATE SCHEMA IF NOT EXISTS NYC_TAXI.SNAPSHOTS;

GRANT ALL ON DATABASE NYC_TAXI TO ROLE SYSADMIN;
GRANT ALL ON SCHEMA NYC_TAXI.RAW TO ROLE SYSADMIN;
```

dbt will create the other schemas automatically when it runs, but I create RAW and SNAPSHOTS manually upfront to avoid permission race conditions.

---

## Step 1: Set Environment Variables

The project reads Snowflake credentials from environment variables — never hardcoded. Add these to your `.zshrc` or `.bashrc`:

```bash
export SNOWFLAKE_ACCOUNT=xy12345.us-east-1   # your account identifier (Snowflake → Settings → Account)
export SNOWFLAKE_USER=your_username
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_ROLE=SYSADMIN               # optional, defaults to SYSADMIN
```

`profiles.yml` is gitignored and reads these env vars at runtime. Never commit credentials.

---

## Step 2: Install Dependencies

```bash
make install
# runs: uv pip install -e ".[dev]"
# installs: dbt-core, dbt-snowflake, pandas, pyarrow, snowflake-connector-python, pytest, ruff
```

---

## Step 3: Install dbt Packages

```bash
make dbt-deps
# runs: dbt deps
# installs dbt_utils and dbt_expectations into dbt_packages/
# dbt_packages/ is gitignored — always regenerated from packages.yml
```

---

## Step 4: Download TLC Data

```bash
make download MONTHS="2024-01 2024-02 2024-03"
# downloads 3 Parquet files from TLC's public cloudfront URL into data/raw/
# each file is ~250MB; ~700MB total
# data/raw/ is gitignored
```

---

## Step 5: Load Data into Snowflake

Run once per month:

```bash
make load MONTH=2024-01
make load MONTH=2024-02
make load MONTH=2024-03
# each run reads the Parquet with pandas, renames columns to snake_case,
# and appends to RAW.YELLOW_TRIPDATA via write_pandas
# expected: ~3M rows per month, ~9.5M rows total
```

Verify in Snowflake:
```sql
SELECT COUNT(*) FROM NYC_TAXI.RAW.YELLOW_TRIPDATA;
-- should return ~24,000,000
```

---

## Step 6: Load Seed Tables

```bash
make dbt-seed
# loads 3 CSV files from seeds/ into Snowflake RAW schema:
#   RAW.TAXI_ZONES     (265 rows)
#   RAW.PAYMENT_TYPES  (6 rows)
#   RAW.VENDOR_NAMES   (3 rows)
```

---

## Step 7: Run dbt Models

```bash
make dbt-run
# runs: dbt run --target dev
# builds all models in dependency order (inferred from ref() calls):
#   1. stg_yellow_trips       → view in DEV_STAGING
#   2. dim_taxi_zones          → table in DEV_MARTS_CORE
#   3. dim_dates               → table in DEV_MARTS_CORE
#   4. fct_trips               → incremental table in DEV_MARTS_CORE
#   5. mart_revenue_by_zone    → table in DEV_MARTS_FINANCE
#   6. mart_hourly_demand      → table in DEV_MARTS_FINANCE
#
# int_trips_enriched is ephemeral — it appears in the compiled SQL but no
# Snowflake object is created for it.
```

---

## Step 8: Run dbt Tests

```bash
make dbt-test
# runs: dbt test --target dev
# executes schema tests (not_null, unique, accepted_values, relationships,
# dbt_expectations) and singular SQL tests from tests/
# tests marked severity: warn will log but not fail the run
```

---

## Step 9: Run the Snapshot (SCD Type 2)

```bash
make dbt-snapshot
# runs: dbt snapshot --target dev
# creates SNAPSHOTS.ZONE_ATTRIBUTES_SNAPSHOT with 265 rows
# all rows have dbt_valid_to = NULL on the first run (no changes yet)
```

To observe SCD2 in action:
1. Edit `seeds/taxi_zones.csv` — change one zone's `borough` value
2. `make dbt-seed` — reload the seed
3. `make dbt-snapshot` — re-run the snapshot
4. Query the snapshot: you will see two rows for that `zone_id` — the old borough row with `dbt_valid_to` set, and a new row with the updated borough and `dbt_valid_to = NULL`

---

## Step 10: Run Pytest Integration Tests

```bash
make pytest
# runs: pytest tests_pytest/ -v
# executes Python assertions against the live Snowflake dev environment
# requires dbt-run to have already populated the schemas
# tests verify: row counts, uniqueness, revenue reconciliation,
#               hourly demand coverage, snapshot integrity
```

---

## Step 11: Generate and View dbt Docs

```bash
make dbt-docs
# runs: dbt docs generate && dbt docs serve
# opens http://localhost:8080 in a browser
# the lineage DAG shows: source → staging → intermediate (ephemeral) → marts
# every column has a description; every test is linked to its model
```

---

## Step 12: Run the Airflow DAG (Orchestrated Pipeline)

I added an Airflow + astronomer-cosmos orchestration layer to show how this pipeline would run in a production environment on a daily schedule.

### First-time setup

```bash
make airflow-init
# initialises the Airflow metadata DB and creates admin user (admin / admin)
```

Before starting Airflow, compile the dbt manifest — cosmos reads it at DAG parse time instead of shelling out to `dbt ls`:

```bash
dbt compile --target dev   # produces target/manifest.json
```

### Start Airflow

```bash
make airflow-start
# starts the Airflow webserver (port 8081) and scheduler in the background
# open http://localhost:8081 — log in with admin / admin
```

### Trigger a manual run

```bash
make airflow-trigger
# triggers the nyc_taxi_pipeline DAG immediately
# or use the "Trigger DAG" button in the Airflow UI
```

### What the DAG does

```
dbt_seed  →  dbt_models (DbtTaskGroup)  →  dbt_snapshot  →  dbt_test
```

The `dbt_models` group uses astronomer-cosmos to expand each dbt model into its own Airflow task, preserving dependency order. Each model can be retried, inspected, and monitored individually in the UI.

### Stop Airflow

```bash
make airflow-stop
```

---

## Backfill — Reprocessing a Specific Date Window

When I need to reprocess `fct_trips` for a specific time range without touching data outside that window:

```bash
make backfill START=2024-01-01 END=2024-01-31
# runs fct_trips with: WHERE pickup_date BETWEEN '2024-01-01' AND '2024-01-31'
# downstream marts are fully rebuilt (--full-refresh is included)
```

---

## Full CI Pass

```bash
make ci
# runs: lint → dbt-seed → dbt-run → dbt-test → pytest
# I run this before committing to verify everything is green
```

---

## Incremental Load — Adding a New Month

When TLC publishes a new month (e.g. April 2024):

```bash
make download MONTHS="2024-04"
make load MONTH=2024-04
make dbt-run
# dbt detects that 2024-04 data is newer than max(pickup_date) in fct_trips
# only April rows are processed — Jan–Mar rows are untouched
```

---

## Switching to Production Target

```bash
dbt run --target prod
# writes to NYC_TAXI database with STAGING, MARTS_CORE, MARTS_FINANCE schemas
# no DEV_ prefix — the generate_schema_name macro handles the difference
```

---

## Troubleshooting

| Problem | Cause | Fix |
|---|---|---|
| `dbt run` fails with "Object 'NYC_TAXI.PUBLIC' does not exist" | Database or schema not created | Run the Step 0 SQL in a Snowflake worksheet |
| `dbt run` fails with "Insufficient privileges" | Role lacks CREATE TABLE | `GRANT ALL ON SCHEMA NYC_TAXI.RAW TO ROLE SYSADMIN` |
| `make load` fails with write_pandas error | Snowflake warehouse suspended | `ALTER WAREHOUSE COMPUTE_WH RESUME;` in Snowflake |
| `make download` returns 404 | TLC hasn't published that month yet | Check the TLC trip record data page for available months |
| Duplicate rows in fct_trips | Same month loaded twice into RAW | Truncate RAW.YELLOW_TRIPDATA and reload all months, or run `make backfill` for that month |
| Airflow DAG import timeout | manifest.json missing or stale | Run `dbt compile --target dev` before starting Airflow |
