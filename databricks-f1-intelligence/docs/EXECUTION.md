# Execution Guide

This guide walks through setting up and running the pipeline from scratch. If you just want to understand how everything fits together, start with [ARCHITECTURE.md](ARCHITECTURE.md) first.

## Prerequisites

- Databricks workspace with **Unity Catalog enabled** (Community Edition works)
- Python 3.10+ and `uv` installed locally
- Databricks CLI v0.200+ (`pip install databricks-cli` or `brew install databricks`)

---

## Initial Setup

### 1. Install dependencies

```bash
cd databricks-f1-intelligence
make install
```

This creates a virtual environment and installs all Python dependencies including `databricks-sdk`, `pyspark`, `streamlit`, and the two API client libraries.

### 2. Authenticate with Databricks

```bash
databricks auth login --host https://your-workspace.cloud.databricks.com
# Follow the OAuth browser flow
```

Verify it worked:
```bash
databricks current-user me
```

### 3. Update `databricks.yml`

Swap in your actual workspace URL:

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

This builds the Python wheel, uploads notebooks and the wheel to your workspace, and registers the job. All resources get prefixed with `[dev your_username]` so they're isolated from any other deployments in the same workspace.

---

## Running the Pipeline

### Step 1: Fetch 2024 season data locally

```bash
make fetch-2024
```

This runs `scripts/fetch_and_upload.py --season 2024 --mode full`, which:
1. Pulls all 24 rounds from the Jolpica-F1 API (rate-limited at ~1 s per call)
2. Pulls race sessions and per-driver lap data from the OpenF1 API (~480 calls total)
3. Saves 8 Parquet files to `data/`
4. Uploads all files to your workspace at `.bundle/f1-intelligence/dev/f1_data/`

The fetch takes 10–20 minutes. If it gets interrupted for any reason, just re-run the same command — it resumes from where it stopped using `data/fetch_progress.json`.

Expected row counts once uploaded:
- `bronze_race_results`: ~480 rows (24 rounds × 20 drivers)
- `bronze_laps`: ~26,000 rows
- `bronze_stints`: ~1,400 rows

### Step 2: Run the Bronze notebook

Open `etl/01_bronze_ingestion.ipynb` in your Databricks workspace and set the widgets:
- `catalog`: `f1_intelligence`
- `schema`: `f1_dev`
- `season`: `2024`
- `mode`: `full`

Run all cells. Each Bronze table is created with CDF and Liquid Clustering enabled on first write. Re-running with the same widgets is safe — the MERGE will find no new rows and leave counts unchanged.

### Step 3: Run the Silver notebook

Open `etl/02_silver_transformation.ipynb` — no widget changes needed, it picks up the same catalog/schema.

Key things happening here:
- `silver_race_results` gets typed columns (`final_position` as Int, `points` as Double), a `status_category` enum, and computed `gap_to_winner_seconds`
- `silver_lap_analysis` joins OpenF1 laps with Jolpica driver/team data by matching `race_date` across the two sources — this is the cross-API join described in ARCHITECTURE.md

### Step 4: Run the Gold notebook

Open `etl/03_gold_aggregation.ipynb` — again, no widget changes needed.

Gold tables created:
- `gold_driver_championship` — 20 rows for 2024 (one per driver), MERGE key `(season, driver_id)`
- `gold_constructor_championship` — 10 rows, MERGE key `(season, constructor_id)`
- `gold_circuit_benchmarks` — 24 rows (one per circuit), MERGE key `circuit_id`
- `gold_tyre_strategy_report` — one row per driver per race, append-only

The final cell demonstrates Delta Time Travel. It queries `gold_driver_championship` as it stood right after Bahrain 2024:

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
-- Race results: ~480 rows for 2024
SELECT COUNT(*), MIN(season), MAX(round)
FROM f1_intelligence.f1_dev.bronze_race_results;

-- Lap data: ~26,000 rows
SELECT COUNT(*), COUNT(DISTINCT session_key) AS sessions
FROM f1_intelligence.f1_dev.bronze_laps;

-- Confirm CDF is enabled
DESCRIBE TABLE EXTENDED f1_intelligence.f1_dev.bronze_race_results;
-- Look for: delta.enableChangeDataFeed = true
```

### Silver

```sql
-- Typed columns and derived fields
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
-- Championship: 20 rows for 2024, ordered by position
SELECT current_position, driver_code, constructor_id,
       current_points, wins, podiums, last_round_processed
FROM f1_intelligence.f1_dev.gold_driver_championship
WHERE season = 2024
ORDER BY current_position;

-- Circuit benchmarks: all 24 circuits present
SELECT circuit_name, all_time_fastest_lap_seconds, fastest_lap_driver_id
FROM f1_intelligence.f1_dev.gold_circuit_benchmarks
ORDER BY all_time_fastest_lap_seconds;

-- Checkpoint state: confirms all three pipelines ran and saved their versions
SELECT pipeline_name, last_processed_version, records_processed, processed_at
FROM f1_intelligence.f1_dev.pipeline_checkpoints
ORDER BY processed_at DESC;
```

### Idempotency check

This is worth running at least once to confirm the MERGE is actually working:

```sql
-- Record the count before
SELECT COUNT(*) FROM f1_intelligence.f1_dev.bronze_race_results;

-- Re-run the Bronze notebook with the same widgets (season=2024, mode=full)

-- Count must be identical — no new rows created
SELECT COUNT(*) FROM f1_intelligence.f1_dev.bronze_race_results;
```

---

## Incremental Load (2025 Round-by-Round)

This is where the CDF and Gold MERGE patterns really show their value. Loading a new round only processes the new rows — not the full table — and the Gold championship rows are updated in-place rather than re-created.

### Step 1: Fetch a single 2025 round

```bash
make fetch-2025-round ROUND=1
```

This fetches only Round 1 data and overwrites the workspace Parquet files.

### Step 2: Run Bronze with updated season/round

Set widgets: `season=2025`, `round=1`, `mode=incremental`

The MERGE inserts new rows for 2025 Round 1. Existing 2024 rows are untouched.

### Step 3: Run Silver and Gold

No widget changes needed. CDF reads only the new Bronze rows committed since the last checkpoint. Gold's `gold_driver_championship` MERGE-INSERTs 20 new rows for season 2025.

### Step 4: Verify the in-place MERGE

```sql
-- After Round 1: 20 rows for 2025, all with last_round_processed = 1
SELECT driver_code, current_points, last_round_processed, updated_at
FROM f1_intelligence.f1_dev.gold_driver_championship
WHERE season = 2025
ORDER BY current_position;
```

Now fetch Round 2 and re-run the pipeline. The same 20 rows will be MERGE-UPDATED — same primary keys, new points totals and updated `updated_at` timestamps:

```sql
-- After Round 2: same 20 rows, updated values
SELECT driver_code, current_points, last_round_processed, updated_at
FROM f1_intelligence.f1_dev.gold_driver_championship
WHERE season = 2025
ORDER BY current_position;
-- current_points and updated_at will have changed; no new rows added
```

### Step 5: Time Travel between rounds

```sql
-- Championship as it stood after Round 1 (day before Round 2 race)
SELECT driver_code, current_position, current_points
FROM f1_intelligence.f1_dev.gold_driver_championship
TIMESTAMP AS OF '2025-03-17'
WHERE season = 2025
ORDER BY current_position;
```

---

## Running via the DAB Job

Instead of running notebooks manually, I can trigger the full pipeline as a job:

```bash
make run-dev
# or with explicit parameters:
databricks bundle run f1_intelligence_job --target dev \
  -p season=2025 -p round=5 -p mode=incremental
```

The job chains `bronze_ingestion → silver_transformation → gold_aggregation` in strict dependency order. If Bronze fails, Silver and Gold won't run.

For production scheduling, add a `schedule` block to `resources/f1_intelligence_job.job.yml` and deploy with `make deploy-prod`.

---

## Running the Streamlit Dashboard Locally

```bash
cd streamlit_app
pip install -r requirements.txt
export DATABRICKS_SERVER_HOSTNAME=your-workspace.cloud.databricks.com
export DATABRICKS_HTTP_PATH=/sql/1.0/warehouses/your-warehouse-id
export DATABRICKS_TOKEN=your-pat-token
export CATALOG=f1_intelligence
export SCHEMA=f1_dev
streamlit run app.py
```

The app connects to the Gold tables in your workspace and renders live data. The `requirements.txt` includes `certifi` to avoid TLS issues on macOS with the Databricks SQL connector.

---

## Tearing Down

```bash
# Remove the deployed job and notebooks from the dev workspace
databricks bundle destroy --target dev

# Drop all tables and schemas (run in a Databricks SQL editor or notebook)
# DROP SCHEMA IF EXISTS f1_intelligence.f1_dev CASCADE;
# DROP SCHEMA IF EXISTS f1_intelligence.f1_prod CASCADE;
# DROP CATALOG IF EXISTS f1_intelligence CASCADE;
```
