# Local Execution Guide

Full walkthrough for running the pipeline locally with Docker. For infrastructure reference see [`LOCAL_DEVELOPMENT_SETUP.md`](LOCAL_DEVELOPMENT_SETUP.md).

---

## Data Flow

```
kinesis_stream_producer.py
    │  (synthetic listen events)
    ▼
LocalStack  (Kinesis mock, port 4566)
    │
    ▼
spark_aggregator.py  (PySpark Structured Streaming)
    │  enriches with songs.csv + users.csv
    │  computes windowed aggregations
    ▼
MinIO  (S3 mock, port 9000)
  aggregations/
    hourly_streams/
    top_tracks_hourly/
    country_metrics_hourly/
```

---

## Step 1 — Start containers

```bash
make up
docker compose ps   # wait until all 3 show healthy
```

The Spark container idles (`tail -f /dev/null`) until you submit a job.

---

## Step 2 — Create the MinIO bucket

MinIO doesn't auto-create buckets. After every `make up`:

1. Open **http://localhost:9001** — login: `minioadmin / minioadmin`
2. Create bucket **`etl-project-2-data`**
3. Upload `songs.csv` and `users.csv` from `sample_data_initial_load/` into the bucket root

> `make down` runs `docker compose down -v` which wipes the `minio-storage` volume — recreate the bucket before each new run.

---

## Step 3 — Run the producer

```bash
make producer
```

Expected output:
```
INFO - Stream 'music-streams' does not exist, creating...
INFO - Stream 'music-streams' is now active
INFO - Starting producer: music-streams | batch=20 | interval=5.0s
INFO - [Batch 1] Sent 20 events (total: 20)
INFO - [Batch 2] Sent 20 events (total: 40)
```

The Kinesis stream is auto-created on first run. Leave the producer running while you start the aggregator.

---

## Step 4 — Run the aggregator

> **Prerequisite:** On a fresh clone, build the connector JAR first:
> ```bash
> make build-kinesis-jar   # ~3 min
> ```

In a second terminal:

```bash
make consumer
```

Expected log output:
```
INFO - Starting Spark session (local mode)
INFO - Reading dimension tables from MinIO...
INFO - songs loaded: 50 rows
INFO - users loaded: 1000 rows
INFO - Starting 3 streaming queries...
INFO - hourly_streams query started
INFO - top_tracks_hourly query started
INFO - country_metrics_hourly query started
INFO - All queries running. Awaiting termination...
```

First Parquet output appears after ~5 minutes (first window closes).

> **TRIM_HORIZON:** The consumer always reads from the start of the shard. If the producer ran first, Spark replays the backlog then catches up to live data. No events are lost regardless of start order — event-time windowing ensures backlogged records land in their correct original windows.

---

## Step 5 — Monitor with Spark UI

Open **http://localhost:4040** once the aggregator is running.

| Tab | What to look for |
|-----|------------------|
| Structured Streaming | 3 active queries: `hourly_streams`, `top_tracks_hourly`, `country_metrics_hourly` |
| Jobs | Streaming micro-batch jobs every ~30s |
| SQL / DataFrame | Join + aggregation query plans |

---

## Step 6 — Browse results in MinIO

**http://localhost:9001** → `etl-project-2-data` → `aggregations/`. After the first window closes, three partition folders appear:

```
etl-project-2-data/
  aggregations/
    hourly_streams/window_start=2026-03-03 12:25:00/part-00000-<uuid>.snappy.parquet
    top_tracks_hourly/window_start=2026-03-03 12:25:00/part-00000-<uuid>.snappy.parquet
    country_metrics_hourly/window_start=2026-03-03 12:25:00/part-00000-<uuid>.snappy.parquet
  checkpoints/
    hourly_streams/
    top_tracks_hourly/
    country_metrics_hourly/
```

---

## Step 7 — Tear down

```bash
make down
```

---

## Minimum Viable Run

**One-time (fresh clone):**
```bash
make build-kinesis-jar
```

**Terminal 1:**
```bash
make up
# wait for healthy, then create bucket in http://localhost:9001 and upload CSVs
make producer
```

**Terminal 2:**
```bash
make consumer
```

---

## Troubleshooting

For every error encountered during local setup see [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md) — Part 1 covers all local Docker issues.
