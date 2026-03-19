# Architecture — F1 Intelligence

## Overview

F1 Intelligence is an end-to-end data engineering platform for Formula 1 analytics, built on Databricks using the medallion architecture pattern. It ingests data from two public APIs, processes it through three layers (Bronze → Silver → Gold), and serves a Streamlit dashboard backed by Unity Catalog tables.

## Data Flow

```
Jolpica-F1 API          OpenF1 API
(results, standings,    (lap timing, tyre
 qualifying, pit stops)  stints, weather)
        │                      │
        ▼                      ▼
   scripts/fetch_and_upload.py  (runs locally)
   • Fetches per-round, per-driver
   • Saves Parquet to data/
   • Uploads via `databricks workspace import --format RAW`
   • Resume-safe: fetch_progress.json tracks completed calls
        │
        ▼
   Workspace Parquet files
   /Workspace/Users/.../.bundle/f1-intelligence/dev/f1_data/
        │
        ▼
┌─────────────────────────────────────────────────────────────┐
│  BRONZE  (Delta, MERGE, CDF-enabled, Liquid Clustered)       │
│                                                              │
│  bronze_race_schedule     bronze_laps                        │
│  bronze_race_results      bronze_stints                      │
│  bronze_qualifying                                           │
│  bronze_pit_stops                                            │
│  bronze_driver_standings                                     │
│  bronze_constructor_standings                                │
└───────────────────────┬─────────────────────────────────────┘
                        │ CDF incremental reads
                        │ (checkpoint: bronze_to_silver_results
                        │              bronze_to_silver_laps)
┌───────────────────────▼─────────────────────────────────────┐
│  SILVER  (typed, enriched, joined, validated)                │
│                                                              │
│  silver_race_results      (Jolpica + schedule join)          │
│  silver_qualifying        (Q-time parsing, gap-to-pole)      │
│  silver_driver_standings  (position_change via LAG)          │
│  silver_constructor_standings  (points_gap_to_leader)        │
│  silver_lap_analysis      (OpenF1 × Jolpica race_date join)  │
└───────────────────────┬─────────────────────────────────────┘
                        │ CDF incremental reads
                        │ (checkpoint: silver_to_gold)
┌───────────────────────▼─────────────────────────────────────┐
│  GOLD  (analytics-ready, genuine MERGE upserts)              │
│                                                              │
│  gold_driver_championship     ← MERGE (season, driver_id)   │
│  gold_constructor_championship ← MERGE (season, constr_id)  │
│  gold_circuit_benchmarks      ← MERGE (circuit_id)          │
│  gold_tyre_strategy_report    ← APPEND per race             │
└───────────────────────┬─────────────────────────────────────┘
                        │
             ┌──────────▼──────────┐
             │  Streamlit Dashboard │
             │  • Championship      │
             │  • Race Results      │
             │  • Tyre Strategy     │
             │  • Circuit Records   │
             └─────────────────────┘
```

## Key Technical Patterns

### 1. Delta MERGE for Idempotency and Upserts

Every Bronze table uses MERGE on a natural key. This means running `fetch_and_upload.py` + the Bronze notebook twice for the same season/round produces exactly the same table state — no duplicates.

**Genuine upsert scenarios** (not just deduplication):

- **Post-race penalties**: The FIA stewards can apply time penalties hours or days after a race (e.g., 5-second unsafe release penalty). Re-fetching and re-running Bronze for that round will MERGE-UPDATE `bronze_race_results.final_position` and `.points` for the penalised driver. The change propagates via CDF through Silver and into the Gold championship MERGE.

- **Championship standings**: `gold_driver_championship` has exactly 1 row per driver per season. After Round 1, this row is inserted. After Round 2, it is MERGE-UPDATED with new points, wins, and position. After all 24 rounds, each row has been updated 23 times — all captured as Delta versions.

- **Circuit benchmarks**: `gold_circuit_benchmarks` updates `all_time_fastest_lap_seconds` only when a new race produces a faster lap at that circuit.

### 2. Change Data Feed (CDF)

CDF is enabled on all Bronze and Silver tables. The `read_incremental_or_full()` helper in `utils/helpers.py` handles the logic:

- **First run**: no checkpoint row → full read
- **Subsequent runs**: reads only `_change_type IN ('insert', 'update_postimage')` rows since `last_processed_version + 1`

Two separate checkpoint entries (`bronze_to_silver_results` and `bronze_to_silver_laps`) allow independent recovery if one path fails. If lap data upload fails for a round but results succeed, the results checkpoint advances while laps stay back.

### 3. Liquid Clustering

All Delta tables use `CLUSTER BY` instead of static `PARTITIONBY` + `ZORDER BY`. Applied via `helpers.enable_liquid_clustering()` immediately after first table creation.

Cluster columns:
- Bronze/Silver: `(season, round, driver_id)` or `(season, round, constructor_id)`
- Gold championships: `(season, driver_id)` or `(season, constructor_id)`
- Gold circuit benchmarks: `(circuit_id)`

At the data volumes in this project (~500 race result rows, ~26k lap rows), Liquid Clustering is a portfolio demonstration of the feature rather than a performance necessity. The `ARCHITECTURE.md` notes this honestly.

### 4. Delta Time Travel

`gold_driver_championship` accumulates one Delta commit per race processed. Using `TIMESTAMP AS OF` with a race date allows querying the championship as it stood at any point in history:

```sql
-- How the 2024 championship stood after Bahrain (before Saudi Arabia)
SELECT driver_code, current_position, current_points
FROM f1_intelligence.f1_dev.gold_driver_championship
TIMESTAMP AS OF '2024-03-03'
WHERE season = 2024
ORDER BY current_position;
```

`TIMESTAMP AS OF` is preferred over `VERSION AS OF` because it is robust to re-runs (re-running for the same round creates an extra Delta version, breaking the version=round assumption, but the timestamp remains stable).

`silver_driver_standings` (one row per driver per round) is the correct source for per-round progression charts. `gold_driver_championship` (current state only) is for KPI metrics.

### 5. Two-Source Join at Silver

`silver_lap_analysis` joins data from two APIs:
- **OpenF1** provides per-lap timing, sector splits, speed traps (`bronze_laps`)
- **OpenF1** also provides tyre stint data: compound, lap_start, lap_end (`bronze_stints`)
- **Jolpica** provides driver→constructor mapping and race schedule (`bronze_race_results`, `bronze_race_schedule`)

The join key between OpenF1 sessions and Jolpica rounds is `race_date`. OpenF1 session `date_start` (truncated to YYYY-MM-DD) is matched to Jolpica `race_date`. Sprint weekends are handled: if no match is found, the session is logged as a warning and excluded rather than causing a failure.

## Unity Catalog Namespace

```
f1_intelligence          ← catalog
├── f1_dev               ← dev schema (all tables prefixed [dev username] in DAB dev mode)
│   ├── bronze_*         ← 8 Bronze tables
│   ├── silver_*         ← 5 Silver tables
│   ├── gold_*           ← 4 Gold tables
│   └── pipeline_checkpoints
└── f1_prod              ← prod schema
    └── (same tables)
```

## Databricks Asset Bundles

`databricks.yml` defines two targets:
- `dev`: development mode — all resources prefixed `[dev username]`, schedules paused
- `prod`: production mode — resources deployed under `pravbala30@protonmail.com`

The job (`resources/f1_intelligence_job.job.yml`) defines three tasks with strict dependency order: `bronze_ingestion → silver_transformation → gold_aggregation`.

Parameters `catalog`, `schema`, `season`, `round`, `mode` are passed from job level down to each notebook via `base_parameters`.

## Community Edition Constraints

Databricks Community Edition executors and driver nodes cannot make outbound HTTP connections to external APIs. All data fetching is performed locally via `scripts/fetch_and_upload.py` and uploaded to the workspace using `databricks workspace import --format RAW`. Notebooks read from the workspace path instead of calling APIs directly.

Public DBFS root is disabled on Community Edition. Workspace import is used instead.
