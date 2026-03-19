# F1 Intelligence — Databricks Medallion Architecture

I built this project to demonstrate production-grade data engineering patterns on Databricks — the kind of real-world design decisions that don't show up in beginner tutorials. The dataset is Formula 1 race data, which I chose deliberately because it has genuine upsert scenarios: post-race steward penalties can change a driver's result hours after the race, and championship standings need to update in-place after every round rather than just accumulate new rows.

The pipeline ingests from two public APIs, processes through a Bronze → Silver → Gold medallion architecture, and serves a Streamlit analytics dashboard backed by Unity Catalog.

## Dashboard

| Home | Championship |
|---|---|
| ![Home](docs/screenshots/home.png) | ![Championship](docs/screenshots/championship.png) |

| Tyre Strategy | Circuit Benchmarks |
|---|---|
| ![Tyre Strategy](docs/screenshots/tyre_strategy.png) | ![Circuit Benchmarks](docs/screenshots/circuit_benchmarks.png) |

## Architecture

```
Jolpica-F1 API          OpenF1 API
(results, standings,    (lap timing, tyre
 qualifying, pit stops)  stints, weather)
        |                      |
        v                      v
   [fetch_and_upload.py — runs locally]
        |
        v
   Workspace Parquet files
        |
        v
┌─────────────────────────────────────────────┐
│  BRONZE  (raw Delta, MERGE idempotency)      │
│  bronze_race_results   bronze_laps           │
│  bronze_qualifying     bronze_stints         │
│  bronze_driver_standings                     │
│  bronze_constructor_standings                │
│  bronze_race_schedule  bronze_pit_stops      │
└──────────────────┬──────────────────────────┘
                   │ CDF incremental reads
┌──────────────────v──────────────────────────┐
│  SILVER  (typed, enriched, validated)        │
│  silver_race_results                         │
│  silver_qualifying                           │
│  silver_driver_standings  (position_change)  │
│  silver_constructor_standings  (points_gap)  │
│  silver_lap_analysis  (Jolpica × OpenF1)     │
└──────────────────┬──────────────────────────┘
                   │ CDF incremental reads
┌──────────────────v──────────────────────────┐
│  GOLD  (analytics-ready, genuine MERGE)      │
│  gold_driver_championship    ← MERGE/round   │
│  gold_constructor_championship ← MERGE/round │
│  gold_circuit_benchmarks    ← conditional    │
│  gold_tyre_strategy_report  ← APPEND/race    │
└─────────────────────────────────────────────┘
                   │
        ┌──────────v──────────┐
        │  Streamlit App       │
        │  • Championship      │
        │  • Race Results      │
        │  • Tyre Strategy     │
        │  • Circuit Records   │
        └─────────────────────┘
```

## Key Patterns Demonstrated

| Pattern | Where and Why |
|---|---|
| **Delta MERGE upserts** | Every Bronze table — ensures re-running for the same round never duplicates rows. Gold standings — one row per driver, updated in-place after each round. |
| **Change Data Feed (CDF)** | Bronze→Silver→Gold incremental reads. Only rows that changed since the last checkpoint are processed, avoiding full table scans on every run. |
| **Liquid Clustering** | Applied to all Delta tables at creation. Replaces the static `PARTITIONED BY` + `ZORDER BY` pattern with auto-managed clustering on the MERGE keys. |
| **Delta Time Travel** | Gold notebook queries `gold_driver_championship TIMESTAMP AS OF` a race date to reconstruct standings at any point in the season. |
| **Two-source join** | `silver_lap_analysis` joins OpenF1 per-lap telemetry with Jolpica driver/team metadata using `race_date` as the bridge key. |
| **Databricks Asset Bundles** | Full dev/prod deployment config in `databricks.yml`. Resources are namespaced per target to avoid dev/prod collisions. |
| **Unity Catalog** | Three-level namespace `f1_intelligence.f1_dev.*` with separate dev and prod schemas. |

## Data Sources

- **[Jolpica-F1 API](https://api.jolpi.ca/ergast/f1/)** — race results, standings, qualifying, pit stops. The maintained successor to the Ergast API, no auth required.
- **[OpenF1 API](https://api.openf1.org/)** — per-lap timing, tyre stints, session weather. Free for historical data (2023+), no auth required.

## Data Scope

- **2024 season**: 24 races, loaded as a full historical batch
- **2025 season**: loaded round-by-round to demonstrate the incremental CDF path and in-place Gold MERGE updates

## Quick Start

```bash
# 1. Install dependencies
make install

# 2. Authenticate with Databricks
databricks auth login --host https://your-workspace.cloud.databricks.com

# 3. Fetch 2024 season data locally and upload to the workspace
make fetch-2024

# 4. Deploy the bundle to dev
make deploy-dev

# 5. Run the pipeline job
make run-dev
```

See [docs/EXECUTION.md](docs/EXECUTION.md) for the full step-by-step guide including verification queries and the incremental load walkthrough. For a deep-dive into the dashboard pages and the Asset Bundle deployment setup, see [docs/DASHBOARD_AND_DEPLOYMENT.md](docs/DASHBOARD_AND_DEPLOYMENT.md).

## Project Structure

```
databricks-f1-intelligence/
├── databricks.yml                    # Bundle config (catalog, schema, app)
├── pyproject.toml                    # Python package + dependencies
├── Makefile                          # Common operations
├── utils/
│   ├── jolpica.py                    # Jolpica-F1 REST client
│   ├── openf1.py                     # OpenF1 REST client
│   ├── schema.py                     # All StructType definitions + MERGE_KEYS
│   ├── helpers.py                    # Delta MERGE, CDF, checkpoints, clustering
│   └── validators.py                 # Per-layer data quality checks
├── etl/
│   ├── 01_bronze_ingestion.ipynb
│   ├── 02_silver_transformation.ipynb
│   └── 03_gold_aggregation.ipynb
├── resources/
│   └── f1_intelligence_job.job.yml
├── scripts/
│   └── fetch_and_upload.py           # Local fetch with resume support
├── streamlit_app/
│   ├── app.py
│   └── pages/
│       ├── 01_championship.py
│       ├── 02_race_results.py
│       ├── 03_tyre_strategy.py
│       └── 04_circuit_benchmarks.py
└── docs/
    ├── ARCHITECTURE.md
    ├── EXECUTION.md
    └── DASHBOARD_AND_DEPLOYMENT.md
```
