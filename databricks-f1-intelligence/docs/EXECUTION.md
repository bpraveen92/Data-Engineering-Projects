# Execution Guide

## Prerequisites

- Databricks workspace with **Unity Catalog enabled** (Community Edition works)
- Python 3.10+ and `uv` installed locally
- Databricks CLI v0.200+ (`pip install databricks-cli` or via brew)

---

## Initial Setup

### 1. Install dependencies

```bash
cd databricks-f1-intelligence
make install
```

### 2. Authenticate with Databricks

```bash
databricks auth login --host https://your-workspace.cloud.databricks.com
# Follow the OAuth browser flow
```

Verify:
```bash
databricks current-user me
```

### 3. Update `databricks.yml`

Replace the workspace host with your actual workspace URL:

```yaml
targets:
  dev:
    workspace:
      host: https://your-workspace.cloud.databricks.com
```

### 4. Deploy to dev

```bash
make deploy-dev
```

This builds the wheel, uploads notebooks and the wheel to your workspace, and registers the job. Resources are prefixed with `[dev your_username]`.

---

## Running the Pipeline

### Step 1: Fetch 2024 season data locally

```bash
make fetch-2024
```

This runs `scripts/fetch_and_upload.py --season 2024 --mode full`, which:
1. Fetches all 24 rounds from the Jolpica-F1 API (rate-limited, ~0.5s between calls)
2. Fetches race sessions and per-driver lap data from the OpenF1 API
3. Saves 8 Parquet files to `data/`
4. Uploads all files to your workspace at `.bundle/f1-intelligence/dev/f1_data/`

If the fetch is interrupted, re-run the same command — it resumes from where it stopped using `data/fetch_progress.json`.

Expected duration: 10–20 minutes (480 OpenF1 API calls for 2024 laps). Expected row counts:
- `bronze_race_results`: ~480 rows (24 rounds × 20 drivers)
- `bronze_laps`: ~26,000 rows
- `bronze_stints`: ~1,400 rows

### Step 2: Run the Bronze notebook

Open `etl/01_bronze_ingestion.ipynb` in your Databricks workspace and set widgets:
- `catalog`: `f1_intelligence`
- `schema`: `f1_dev`
- `season`: `2024`
- `mode`: `full`

Run all cells. Each Bronze table will be created with CDF and Liquid Clustering enabled.

### Step 3: Run the Silver notebook

Open `etl/02_silver_transformation.ipynb`. No widget changes needed.

Key Silver tables created:
- `silver_race_results` — typed, status-categorized, gap-to-winner computed
- `silver_lap_analysis` — OpenF1 laps joined with Jolpica driver/team data via race_date

### Step 4: Run the Gold notebook

Open `etl/03_gold_aggregation.ipynb`. No widget changes needed.

Gold tables created:
- `gold_driver_championship` — 20 rows (one per 2024 driver), MERGE on `(season, driver_id)`
- `gold_constructor_championship` — 10 rows, MERGE on `(season, constructor_id)`
- `gold_circuit_benchmarks` — 24 rows (one per circuit), MERGE on `circuit_id`
- `gold_tyre_strategy_report` — one row per driver per race

The final cell demonstrates **Delta Time Travel**:
```sql
SELECT driver_code, current_position, current_points
FROM f1_intelligence.f1_dev.gold_driver_championship
TIMESTAMP AS OF '2024-03-03'
WHERE season = 2024
ORDER BY current_position;
```

---

## Verifying Each Layer

### Bronze

```sql
-- Race results: should be ~480 rows for 2024
SELECT COUNT(*), MIN(season), MAX(round)
FROM f1_intelligence.f1_dev.bronze_race_results;

-- Lap data: should be ~26,000 rows
SELECT COUNT(*), COUNT(DISTINCT session_key) AS sessions
FROM f1_intelligence.f1_dev.bronze_laps;

-- CDF enabled check
DESCRIBE TABLE EXTENDED f1_intelligence.f1_dev.bronze_race_results;
-- Look for: delta.enableChangeDataFeed = true
```

### Silver

```sql
-- Race results with derived columns
SELECT season, round, driver_code, constructor_id,
       final_position, points, status_category, fastest_lap_seconds
FROM f1_intelligence.f1_dev.silver_race_results
ORDER BY season, round, final_position
LIMIT 20;

-- Lap analysis: compound and tyre age should be populated
SELECT session_key, driver_code, lap_number, lap_duration_seconds,
       compound, tyre_age_laps, is_personal_best
FROM f1_intelligence.f1_dev.silver_lap_analysis
WHERE compound IS NOT NULL
LIMIT 20;
```

### Gold

```sql
-- Championship: should have 20 rows for 2024
SELECT current_position, driver_code, constructor_id,
       current_points, wins, podiums, last_round_processed
FROM f1_intelligence.f1_dev.gold_driver_championship
WHERE season = 2024
ORDER BY current_position;

-- Circuit benchmarks: all 24 circuits present
SELECT circuit_name, all_time_fastest_lap_seconds, fastest_lap_driver_id
FROM f1_intelligence.f1_dev.gold_circuit_benchmarks
ORDER BY all_time_fastest_lap_seconds;

-- Checkpoints saved
SELECT pipeline_name, last_processed_version, records_processed, processed_at
FROM f1_intelligence.f1_dev.pipeline_checkpoints
ORDER BY processed_at DESC;
```

### Idempotency check

Re-run the Bronze notebook for the same season — row counts must stay identical:

```sql
SELECT COUNT(*) FROM f1_intelligence.f1_dev.bronze_race_results;
-- Run Bronze notebook again with same widgets
SELECT COUNT(*) FROM f1_intelligence.f1_dev.bronze_race_results;
-- Both counts must match
```

---

## Incremental Load (2025 Round-by-Round)

### Step 1: Fetch a single 2025 round

```bash
make fetch-2025-round ROUND=1
```

This fetches only Round 1 data and overwrites the workspace Parquet files.

### Step 2: Run Bronze notebook with updated season/round

Set widgets: `season=2025`, `round=1`, `mode=incremental`

The MERGE will INSERT new rows for 2025 Round 1 drivers/results.

### Step 3: Run Silver and Gold notebooks

No widget changes needed. CDF reads only the new Bronze rows. Gold's `gold_driver_championship` MERGE-INSERTs 20 new rows for season 2025, then MERGE-UPDATEs them after each subsequent round.

### Step 4: Verify the MERGE upsert

```sql
-- After Round 1
SELECT driver_code, current_points, last_round_processed, _updated_at
FROM f1_intelligence.f1_dev.gold_driver_championship
WHERE season = 2025
ORDER BY current_position;

-- Fetch Round 2, run pipeline again, then check _updated_at changed:
SELECT driver_code, current_points, last_round_processed, _updated_at
FROM f1_intelligence.f1_dev.gold_driver_championship
WHERE season = 2025
ORDER BY current_position;
-- Same 20 rows, but current_points and _updated_at are updated
```

### Step 5: Delta Time Travel — compare standings after Round 1 vs Round 2

```sql
-- Championship after Round 1 (use the race date of Round 2 minus 1 day)
SELECT driver_code, current_position, current_points
FROM f1_intelligence.f1_dev.gold_driver_championship
TIMESTAMP AS OF '2025-03-17'  -- day before Saudi 2025
WHERE season = 2025
ORDER BY current_position;
```

---

## Scheduling

The job is configured to run manually (schedule paused in dev mode by DAB). To trigger:

```bash
make run-dev
# or with parameters:
databricks bundle run f1_intelligence_job --target dev \
  -p season=2025 -p round=5 -p mode=incremental
```

For production scheduling, edit `resources/f1_intelligence_job.job.yml` to add a `schedule` block and deploy with `make deploy-prod`.

---

## Tearing Down

```bash
# Remove deployed job and notebooks from dev workspace
databricks bundle destroy --target dev

# Drop the Unity Catalog schema and tables
# (run in a Databricks SQL editor or notebook)
# DROP SCHEMA IF EXISTS f1_intelligence.f1_dev CASCADE;
# DROP SCHEMA IF EXISTS f1_intelligence.f1_prod CASCADE;
# DROP CATALOG IF EXISTS f1_intelligence CASCADE;
```
