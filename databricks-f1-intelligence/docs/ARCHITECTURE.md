# Architecture — F1 Intelligence

## Overview

F1 Intelligence is an end-to-end data pipeline and analytics platform for Formula 1 data, built on Databricks. I'm a huge fan of the sport, but beyond that, Formula 1 is a great domain for this project because the data has genuine update scenarios — not just inserts. Post-race steward penalties alter results hours after a race ends, championship standings get recomputed after every round, and circuit all-time records are conditionally overwritten when a faster lap is set. These are the cases where Delta MERGE is the right tool — the pipeline has to handle updates to existing rows, not just new inserts.

The pipeline follows the medallion architecture (Bronze → Silver → Gold) and is deployed via Databricks Asset Bundles with a Unity Catalog-backed namespace.

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

Every Bronze table uses MERGE on a natural key. This means I can safely re-run `fetch_and_upload.py` and the Bronze notebook for the same season/round without worrying about duplicates — it'll produce exactly the same table state both times.

More importantly, the MERGE enables genuine update propagation for three real-world scenarios:

**Post-race steward penalties**: The FIA can apply time penalties hours or even days after a race finishes (unsafe release, track limits violations, etc.). When this happens, I re-fetch that round and re-run Bronze. The MERGE updates `bronze_race_results.final_position` and `.points` for the penalised driver. Because CDF is enabled, the changed row flows through Silver and into the Gold championship recalculation automatically — no manual intervention needed.

**Championship standings**: `gold_driver_championship` has exactly one row per driver per season. After Round 1, that row is inserted with the driver's points. After Round 2, it's MERGE-UPDATED with cumulative points, wins, and current position. By the end of a 24-round season, each row has been updated 23 times. Every update is captured as a separate Delta commit, which is what makes Time Travel meaningful.

**Circuit benchmarks**: `gold_circuit_benchmarks` only updates `all_time_fastest_lap_seconds` when a new race produces a faster lap at that circuit — conditional update logic built into the Gold notebook before the MERGE.

### 2. Change Data Feed (CDF)

CDF is enabled on all Bronze and Silver tables immediately after they're created (via `helpers.enable_cdf()`). The `read_incremental_or_full()` function in `utils/helpers.py` handles the read logic:

- **First run**: no checkpoint row exists → performs a full table read
- **Subsequent runs**: reads only `_change_type IN ('insert', 'update_postimage')` rows since `last_processed_version + 1`

I deliberately use `update_postimage` rather than `update_preimage` — I only want the final state of changed rows, not the before-image. This is especially important for penalty corrections: the postimage carries the corrected position and points, which is exactly what Silver and Gold need.

I maintain two separate checkpoint entries — `bronze_to_silver_results` and `bronze_to_silver_laps` — instead of a single one. This means if lap data fails to upload for a round but results succeed, the results checkpoint advances while laps stay at the previous version. The two paths recover independently on the next run.

### 3. Liquid Clustering

All Delta tables use `CLUSTER BY` rather than the older `PARTITIONED BY` + `ZORDER BY` combination. I apply Liquid Clustering via `helpers.enable_liquid_clustering()` immediately after the first table write.

Cluster columns are chosen to match the MERGE predicate, so Delta can skip irrelevant files during MERGE scans:
- Bronze/Silver result tables: `(season, round, driver_id)`
- Bronze/Silver standings: `(season, round, constructor_id)` for constructor tables
- Gold championships: `(season, driver_id)` or `(season, constructor_id)`
- Gold circuit benchmarks: `(circuit_id)`

At the data volumes here (~500 race result rows, ~26k lap rows), the clustering benefit is marginal. The real payoff comes at scale — hundreds of seasons or sub-lap telemetry at full granularity.

### 4. Delta Time Travel

`gold_driver_championship` accumulates one Delta commit per race processed. Because each MERGE creates a new version, I can use `TIMESTAMP AS OF` to reconstruct the championship standings as they stood after any given race:

```sql
-- Championship standings after Bahrain 2024 (before Saudi Arabia)
SELECT driver_code, current_position, current_points
FROM f1_intelligence.f1_dev.gold_driver_championship
TIMESTAMP AS OF '2024-03-03'
WHERE season = 2024
ORDER BY current_position;
```

I prefer `TIMESTAMP AS OF` over `VERSION AS OF` because it's robust to re-runs. If I re-run the pipeline for the same round, that creates an extra Delta version — breaking any assumption that version number equals round number. The timestamp stays stable regardless of how many times the pipeline runs.

`gold_driver_championship` stores the current state only (one row per driver per season). For per-round progression charts in the Streamlit dashboard, I query `silver_driver_standings` instead, which has one row per driver per round.

### 5. Two-Source Join at Silver

`silver_lap_analysis` combines data from two different APIs that use completely different identifiers:

- **OpenF1** uses `session_key` to identify race sessions — `bronze_laps` and `bronze_stints` both reference this
- **Jolpica** uses `season` + `round` — `bronze_race_results` and `bronze_race_schedule` reference these

There's no shared key between the two systems, so I join them on `race_date`. OpenF1's `date_start` is truncated to `YYYY-MM-DD` and matched against Jolpica's `race_date`. This resolves `session_key → round` for every session, which lets me attach driver/constructor metadata and round numbers to every lap record.

Sprint weekends are a minor edge case: Jolpica serves a 404 for qualifying on those 6 rounds in 2024, so `fetch_qualifying()` returns an empty list, and the Silver qualifying notebook exits early if it receives no CDF rows. No errors, no manual handling needed.

## Unity Catalog Namespace

```
f1_intelligence          ← catalog
├── f1_dev               ← dev schema
│   ├── bronze_*         ← 8 Bronze tables
│   ├── silver_*         ← 5 Silver tables
│   ├── gold_*           ← 4 Gold tables
│   └── pipeline_checkpoints
└── f1_prod              ← prod schema
    └── (same tables)
```

The catalog and schema are created by `helpers.get_or_create_catalog_schema()` at the top of every notebook, so the pipeline is fully self-bootstrapping on a fresh workspace.

## Databricks Asset Bundles

`databricks.yml` defines two deployment targets:

- **`dev`**: resources prefixed with `[dev username]` so multiple developers can work in the same workspace without colliding. Job schedules are automatically paused.
- **`prod`**: resources deployed without prefix under the service account.

The job in `resources/f1_intelligence_job.job.yml` chains three tasks with strict dependency ordering: `bronze_ingestion → silver_transformation → gold_aggregation`. Parameters `catalog`, `schema`, `season`, `round`, and `mode` are defined at the job level and passed into each notebook via `base_parameters`.

## Why Data Is Fetched Locally

Databricks Community Edition executor and driver nodes can't make outbound HTTP calls to external APIs. Rather than work around this with a proxy or VPN, I fetch all data locally via `scripts/fetch_and_upload.py` and upload the resulting Parquet files to the workspace using `databricks workspace import --format RAW`. Notebooks then read from that workspace path instead of calling APIs directly.

This pattern also has a practical advantage: it separates the API rate-limiting concern (handled locally, with resume support) from the cluster compute concern (notebooks just read Parquet). Even in a production environment, it's a reasonable pattern for APIs with strict rate limits or auth requirements that are easier to manage outside the cluster.
