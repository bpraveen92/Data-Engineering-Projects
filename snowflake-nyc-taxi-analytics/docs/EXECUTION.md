# Execution Guide — NYC Taxi Analytics (dbt + Snowflake)

## Prerequisites

- Python 3.10+
- uv (`pip install uv` or `brew install uv`)
- Snowflake account (free trial at snowflake.com — 30 days, no credit card)
- `dbt` CLI (installed as part of `make install`)

---

## Step 0: Snowflake Setup

Log into your Snowflake account and run the following in a Snowflake worksheet
to create the database and schemas:

```sql
-- Create the database
CREATE DATABASE IF NOT EXISTS NYC_TAXI;

-- Create schemas (dbt will also create these automatically via generate_schema_name,
-- but creating them manually first avoids a permissions race condition)
CREATE SCHEMA IF NOT EXISTS NYC_TAXI.RAW;
CREATE SCHEMA IF NOT EXISTS NYC_TAXI.SNAPSHOTS;

-- Grant permissions (replace SYSADMIN with your role if different)
GRANT ALL ON DATABASE NYC_TAXI TO ROLE SYSADMIN;
GRANT ALL ON SCHEMA NYC_TAXI.RAW TO ROLE SYSADMIN;
```

---

## Step 1: Set Environment Variables

The project reads Snowflake credentials from environment variables.
Add these to your shell profile (`.zshrc` or `.bashrc`) or a local `.env` file:

```bash
export SNOWFLAKE_ACCOUNT=xy12345.us-east-1   # your account identifier (Settings → Account)
export SNOWFLAKE_USER=your_username
export SNOWFLAKE_PASSWORD=your_password
export SNOWFLAKE_ROLE=SYSADMIN               # optional, defaults to SYSADMIN
```

**Note:** `profiles.yml` is gitignored.  It reads these env vars at runtime.
Never hardcode credentials into any file.

---

## Step 2: Install Dependencies

```bash
make install
# This runs: uv pip install -e ".[dev]"
# Installs: dbt-core, dbt-snowflake, pandas, pyarrow, snowflake-connector,
#           pytest, ruff — all pinned in pyproject.toml
```

---

## Step 3: Install dbt Packages

```bash
make dbt-deps
# This runs: dbt deps
# Installs dbt_utils and dbt_expectations into dbt_packages/
# (gitignored — this directory is always regenerated)
```

---

## Step 4: Download TLC Data

```bash
make download MONTHS="2024-01 2024-02 2024-03"
# Downloads 3 Parquet files from TLC's public cloudfront URL
# Files land in data/raw/ (gitignored — ~700MB total)
# Each file takes ~1-2 minutes depending on connection speed
```

---

## Step 5: Load Data into Snowflake

Run this three times, once per month:

```bash
make load MONTH=2024-01
make load MONTH=2024-02
make load MONTH=2024-03
# Each run: pandas reads the Parquet → renames columns → appends to RAW.YELLOW_TRIPDATA
# Expected: ~8M rows per month, ~24M rows total after all three loads
# verify in Snowflake: SELECT COUNT(*) FROM NYC_TAXI.RAW.YELLOW_TRIPDATA;
```

---

## Step 6: Load Seed Tables

```bash
make dbt-seed
# Loads 3 CSV files from seeds/ into Snowflake RAW schema:
#   RAW.TAXI_ZONES       (265 rows)
#   RAW.PAYMENT_TYPES    (6 rows)
#   RAW.VENDOR_NAMES     (3 rows)
# These seed tables are used as dimension sources by dbt models
```

---

## Step 7: Run dbt Models

```bash
make dbt-run
# Runs all models in dependency order (inferred from ref() calls):
#   1. stg_yellow_trips        (view in DEV_STAGING)
#   2. dim_taxi_zones           (table in DEV_MARTS_CORE)
#   3. dim_dates                (table in DEV_MARTS_CORE)
#   4. fct_trips                (incremental table in DEV_MARTS_CORE)
#   5. mart_revenue_by_zone     (table in DEV_MARTS_FINANCE)
#   6. mart_hourly_demand       (table in DEV_MARTS_FINANCE)
#
# Note: int_trips_enriched is ephemeral — it appears in the logs but
# no Snowflake object is created for it.
```

---

## Step 8: Run dbt Tests

```bash
make dbt-test
# Runs schema tests (not_null, unique, accepted_values, relationships,
# dbt_expectations) + singular SQL tests from tests/
# All tests should pass on clean data.
```

---

## Step 9: Run Snapshot (SCD Type 2)

```bash
make dbt-snapshot
# Creates SNAPSHOTS.ZONE_ATTRIBUTES_SNAPSHOT with 265 rows
# All rows will have dbt_valid_to = NULL (all current, no changes yet)

# To observe SCD2 in action:
#   1. Edit seeds/taxi_zones.csv — change one zone's borough value
#   2. make dbt-seed    (reload seed)
#   3. make dbt-snapshot (re-run snapshot)
#   4. Query: SELECT * FROM NYC_TAXI.SNAPSHOTS.ZONE_ATTRIBUTES_SNAPSHOT
#             WHERE zone_id = <changed_zone_id>
#   You will see 2 rows: one expired (dbt_valid_to set), one current (dbt_valid_to NULL)
```

---

## Step 10: Run Pytest Integration Tests

```bash
make pytest
# Runs tests_pytest/ against your live Snowflake dev environment
# Requires dbt-run to have already populated the schemas
# Tests verify: row counts, uniqueness, revenue reconciliation,
#               hourly demand coverage, snapshot integrity
```

---

## Step 11: Generate and View dbt Docs

```bash
make dbt-docs
# Runs: dbt docs generate && dbt docs serve
# Opens browser at http://localhost:8080
# The lineage DAG shows:
#   source → staging → intermediate (ephemeral) → marts → exposures
# Every column has a description; every test is linked to its model
```

---

## Backfill — Reprocessing a Specific Date Window

Use this when you need to reprocess fct_trips for a specific time range without
touching data outside that window:

```bash
make backfill START=2024-01-01 END=2024-01-31
# Reruns fct_trips with: WHERE pickup_date BETWEEN '2024-01-01' AND '2024-01-31'
# Downstream marts will be fully rebuilt (--full-refresh flag is included)
```

---

## Full CI Pass

```bash
make ci
# Runs: lint → dbt-seed → dbt-run → dbt-test → pytest
# Use this before committing to verify everything is green
```

---

## Incremental Load (Adding a New Month)

When TLC publishes a new month's data (e.g. April 2024):

```bash
make download MONTHS="2024-04"
make load MONTH=2024-04
make dbt-run
# dbt detects that 2024-04 data is newer than max(pickup_date) in fct_trips
# Only April rows are processed — Jan–Mar rows are untouched
```

---

## Switching to Production Target

```bash
dbt run --target prod
# Writes to NYC_TAXI_PROD database with STAGING, MARTS_CORE, MARTS_FINANCE schemas
# (no DEV_ prefix — generate_schema_name macro handles the difference)
```

---

## Troubleshooting

| Problem | Cause | Fix |
|---|---|---|
| `dbt run` fails with "Object 'NYC_TAXI.PUBLIC' does not exist" | Database or schema not created | Run the Step 0 SQL in Snowflake worksheet |
| `dbt run` fails with "Insufficient privileges" | Role lacks CREATE TABLE permission | Grant `ALL ON SCHEMA` to your role |
| `make load` fails with "write_pandas error" | Snowflake warehouse is suspended | Run `ALTER WAREHOUSE COMPUTE_WH RESUME;` |
| `make download` returns 404 | TLC hasn't published that month yet | Check https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page for available dates |
| `profiles.yml not found` | profiles.yml is gitignored | Recreate it from the template in `profiles.yml` (already in the repo root) |
| Duplicate rows in fct_trips | Same month loaded twice | Truncate RAW.YELLOW_TRIPDATA and reload, OR run `make backfill` for that month |
