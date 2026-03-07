# How the ETL-Project-2 Pipeline Works — Local Execution Guide

This guide walks through running the full pipeline locally using Docker. Everything runs in containers — no AWS account needed.

For infrastructure reference (LocalStack, MinIO, networking, env vars) see [`LOCAL_DEVELOPMENT_SETUP.md`](LOCAL_DEVELOPMENT_SETUP.md).  
For deploying to real AWS see [`AWS_PRODUCTION_DEPLOYMENT.md`](AWS_PRODUCTION_DEPLOYMENT.md).

---

## Pipeline Data Flow

A synthetic music streaming service generates listen events in real-time. The pipeline captures those events via Kinesis, enriches them with song and user metadata, and produces three windowed aggregations — hourly stream counts, top tracks, and country-level metrics — written as Parquet to S3.

Locally, **LocalStack** mocks AWS Kinesis and **MinIO** mocks AWS S3. The same `spark_aggregator.py` that runs in Docker also runs on AWS EMR Serverless — the only difference is the `--local` flag.

```
kinesis_stream_producer.py
        |
        |  (puts events onto stream)
        v
LocalStack  (Kinesis mock, port 4566)
        |
        |  (Spark reads stream)
        v
spark_aggregator.py  (PySpark Structured Streaming)
        |
        |  enriches with songs.csv + users.csv (from MinIO)
        |  computes windowed aggregations
        v
MinIO  (S3 mock)
  aggregations/
    hourly_streams/
    top_tracks_hourly/
    country_metrics_hourly/
```

---

## Step 1 — Start the containers

```bash
make up
```

Starts three containers on a shared bridge network (`etl-network`):

| Container | Purpose | Port |
|-----------|---------|------|
| `etl-project-2-spark` | Runs the PySpark job | 4040 (Spark UI) |
| `etl-project-2-localstack` | Mocks AWS Kinesis | 4566 |
| `etl-project-2-minio` | Mocks AWS S3 | 9000 (API), 9001 (console) |

Wait until all three show as healthy:

```bash
docker compose ps
```

The Spark container idles (running `tail -f /dev/null`) until you submit a job to it.

---

## Step 2 — Set up MinIO buckets

MinIO doesn't auto-create buckets. After every `make up` you need to create them manually.

1. Open **http://localhost:9001** — log in with `minioadmin / minioadmin`
2. Create bucket **`etl-project-2-data`** with the following structure:
   - Upload `sample_data_initial_load/songs.csv` → `etl-project-2-data/songs.csv`
   - Upload `sample_data_initial_load/users.csv` → `etl-project-2-data/users.csv`
   - Leave `aggregations/` and `checkpoints/` empty (Spark creates them on first write)

> **Note:** `make down` runs `docker compose down -v` which wipes the `minio-storage` volume. The bucket and all uploaded files are lost — recreate them before each new run.

---

## Step 3 — Run the producer

The producer generates synthetic listen events and puts them onto the `music-streams` Kinesis stream in LocalStack:

```bash
make producer
```

Which runs inside `etl-project-2-spark`:

```bash
docker exec etl-project-2-spark python /opt/spark/scripts/kinesis_stream_producer.py \
  --stream-name music-streams \
  --batch-size 20 \
  --interval-seconds 5.0 \
  --local
```

Expected output:

```
INFO - Stream 'music-streams' does not exist, creating...
INFO - Stream 'music-streams' is now active
INFO - Starting producer: music-streams | batch=20 | interval=5.0s
INFO - [Batch 1] Sent 20 events (total: 20)
INFO - [Batch 2] Sent 20 events (total: 40)
...
```

The Kinesis stream is auto-created on first run. Leave the producer running while you start the aggregator.

---

## Step 4 — Run the Spark aggregator

In a new terminal:

```bash
make consumer
```

This runs `spark-submit` inside `etl-project-2-spark` (detached), then tails `/tmp/consumer.log`. The job reads `songs.csv` and `users.csv` from the container's local volume mount (`/opt/spark/sample_data_initial_load/`), consumes the Kinesis stream, computes three windowed aggregations, and writes Parquet to `s3a://etl-project-2-data/aggregations/`.

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

After the first window closes (5 minutes by default), Spark flushes the first Parquet files to MinIO.

**First-run behaviour — `TRIM_HORIZON`:** The Kinesis read always starts from the beginning of the shard. If the producer ran before the consumer was started, Spark replays all previously published records in the first few micro-batches before catching up to the live stream. No events are lost regardless of start order. Because Spark uses **event time** (the `timestamp` field embedded in each record) for windowing — not processing time — backlog records are bucketed into their correct original windows.

---

## Step 5 — Monitor with Spark UI

Open **http://localhost:4040** once the aggregator is running.

| Tab | What to look for |
|-----|------------------|
| **Structured Streaming** | 3 active queries: `hourly_streams`, `top_tracks_hourly`, `country_metrics_hourly` |
| **Jobs** | Streaming micro-batch jobs triggering every ~30s |
| **SQL / DataFrame** | Query plans for the join + aggregation |
| **Executors** | Memory usage |

Each micro-batch shows input rows from Kinesis, processing time, and output rows written to MinIO.

---

## Step 6 — Browse results in MinIO

Open **http://localhost:9001** → `etl-project-2-data` bucket → `aggregations/`. After the first window closes, Parquet files appear partitioned by `window_start`:

```
etl-project-2-data/
  aggregations/
    hourly_streams/
      window_start=2026-03-03 12:25:00/
        part-00000-<uuid>.c000.snappy.parquet
    top_tracks_hourly/
      window_start=2026-03-03 12:25:00/
        part-00000-<uuid>.c000.snappy.parquet
    country_metrics_hourly/
      window_start=2026-03-03 12:25:00/
        part-00000-<uuid>.c000.snappy.parquet
  checkpoints/
    hourly_streams/   ← Spark recovery state
    top_tracks_hourly/
    country_metrics_hourly/
```

---

## Step 7 — Shut down

```bash
make down
```

Stops all containers and wipes the `minio-storage` volume. The next `make up` starts completely clean.

---

## Running It All Together

Minimum viable run — across two terminals:

**Terminal 1:**
```bash
make up
# wait for healthy, then set up MinIO buckets (Step 2)
make producer
```

**Terminal 2:**
```bash
make consumer
```

---

## Key Takeaways

- The **producer** and **consumer** both run inside `etl-project-2-spark` via `docker exec` — no local Python or Spark install required
- The **`--local` flag** switches `spark_aggregator.py` to LocalStack + MinIO mode; omitting it targets real AWS
- **MinIO buckets must be created manually** after every `make up` — the pipeline does not auto-create them
- **`TRIM_HORIZON`** means the consumer always replays from the start of the shard — start order doesn't matter
- **Event time windowing** means backlogged records land in the correct historical windows, not the current one

---

## Troubleshooting

For every error encountered during local setup (JVM credential issues, MinIO 403s, `NoSuchBucket`, LocalStack hostname resolution, etc.) see [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md) — Part 1 covers all local Docker issues, Part 2 covers AWS EMR Serverless.
