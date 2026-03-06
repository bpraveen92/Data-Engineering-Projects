# Real-Time Music Streaming Analytics Pipeline

This is the streaming counterpart to ETL-Project-1. Where that project handled batch CSV files landing in S3, here I'm dealing with continuous music events flowing through Kinesis — and I need to produce windowed aggregations in near real-time. The whole thing runs in Docker locally using LocalStack and MinIO, so there's no AWS account needed to test it.

**Stack**: PySpark 3.5 + AWS Kinesis + Parquet to S3  
**Local Testing**: Docker — Spark, LocalStack (Kinesis), MinIO (S3)  
**Production Target**: EMR / AWS Glue + Kinesis + S3  

> For the full step-by-step run guide and troubleshooting, see [`docs/EXECUTION.md`](docs/EXECUTION.md).  
> For environment variables and config reference, see [`docs/LOCAL_DEVELOPMENT_SETUP.md`](docs/LOCAL_DEVELOPMENT_SETUP.md).

---

## What It Does

The pipeline reads `{user_id, track_id, timestamp, event_type}` events from Kinesis, enriches them with song and user dimension data via broadcast joins, and computes three windowed aggregations every 5 minutes:

| Output Table | What it tells me |
|---|---|
| `hourly_streams/` | Stream counts per track and country per window |
| `top_tracks_hourly/` | Tracks ordered by total stream count per window |
| `country_metrics_hourly/` | Unique users and tracks per country per window |

Results land as partitioned Parquet in MinIO (locally) or S3 (production), queryable via Athena.

---

## Architecture

```
Kinesis Stream
    │  {user_id, track_id, timestamp, event_type}
    ▼
PySpark Structured Streaming
    ├── Broadcast join → songs.csv  (S3/MinIO)
    ├── Broadcast join → users.csv  (S3/MinIO)
    └── Windowed aggregations (5-min window, 1-min watermark)
         ├── hourly_streams/
         ├── top_tracks_hourly/
         └── country_metrics_hourly/
                    │
              MinIO / S3 — Parquet, partitioned by window_start
                    │
              Glue Crawler → Athena
```

---

## Project Structure

```
ETL-Project-2/
├── docker-compose.yml               # 3-container local stack
├── Dockerfile.spark                 # Spark image with JAR dependencies
├── Makefile                         # Common dev commands
├── .env.example                     # Config template — copy to .env
│
├── jars/                            # Spark JARs — not committed (see Prerequisites below)
│   ├── spark-streaming-sql-kinesis-connector_2.12-1.0.0.jar
│   ├── hadoop-aws-3.3.4.jar
│   └── aws-java-sdk-bundle-1.12.565.jar
│
├── scripts/
│   ├── kinesis_stream_producer.py   # Generates synthetic events and sends to Kinesis
│   └── spark_aggregator.py          # Main PySpark aggregation job
│
├── src/utils/
│   ├── dimension_loader.py          # Loads songs/users from S3
│   └── event_parser.py              # Parses raw Kinesis JSON events
│
├── sample_data_initial_load/        # Seed data — volume-mounted into the Spark container
│   ├── songs.csv                    # Track metadata
│   ├── users.csv                    # User info with country
│   └── streams.csv                  # Sample streaming events for the producer
│
└── docs/
    ├── EXECUTION.md                 # Detailed run guide with troubleshooting
    └── LOCAL_DEVELOPMENT_SETUP.md  # Config and environment reference
```

---

## Running on Docker

### Prerequisites — Download JARs

The Spark JARs are **not committed to this repo** (too large for GitHub). Download them once and place them in the `jars/` folder before running anything:

```bash
mkdir -p jars

# 1. Kinesis connector for Spark Structured Streaming
curl -L -o jars/spark-streaming-sql-kinesis-connector_2.12-1.0.0.jar \
  https://github.com/awslabs/spark-sql-kinesis-connector/releases/download/v1.0.0/spark-streaming-sql-kinesis-connector_2.12-1.0.0.jar

# 2. Hadoop AWS S3A filesystem implementation
curl -L -o jars/hadoop-aws-3.3.4.jar \
  https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/3.3.4/hadoop-aws-3.3.4.jar

# 3. AWS Java SDK bundle (required by both connectors above)
curl -L -o jars/aws-java-sdk-bundle-1.12.565.jar \
  https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.565/aws-java-sdk-bundle-1.12.565.jar
```

After downloading, verify:
```bash
ls -lh jars/
# spark-streaming-sql-kinesis-connector_2.12-1.0.0.jar  ~73 MB
# hadoop-aws-3.3.4.jar                                   ~8 MB
# aws-java-sdk-bundle-1.12.565.jar                       ~331 MB
```

> These JARs are listed in `.gitignore` and must be present **before** running `make up` — the Docker build copies them into the image at `COPY jars/ /opt/spark/jars/`.

---

The only prerequisite is Docker Desktop. No local Python, boto3, or Spark install needed — everything runs inside the containers.

### Step 1: Start the containers

```bash
make up
docker compose ps   # wait until all 3 containers show healthy
```

This brings up:
- **`etl-project-2-spark`** — runs the PySpark job (Spark UI at http://localhost:4040)
- **`etl-project-2-localstack`** — simulates AWS Kinesis on port 4566
- **`etl-project-2-minio`** — S3-compatible storage (console at http://localhost:9001)

### Step 2: Set up MinIO buckets and upload dimension data

Open the MinIO console at **http://localhost:9001** and log in with `minioadmin` / `minioadmin`. Then:

1. Create two buckets: **`etl-data`** and **`aggregations`**
2. Inside `etl-data`, upload `sample_data_initial_load/songs.csv` and `sample_data_initial_load/users.csv`

The aggregator reads dimension tables directly from `s3a://etl-data/songs.csv` and `s3a://etl-data/users.csv` — the same `s3a://` path pattern it will use against real S3 in production. Locally, the S3A connector is pointed at MinIO; in production it hits AWS S3. No code changes needed between environments, only the endpoint configuration changes via the `--local` flag.

> ⚠️ **Both buckets must be created manually before starting the consumer.** The pipeline does **not** auto-create them. If `aggregations` is missing when the consumer starts, Spark will crash with a `NoSuchBucket` error on the first write attempt. If `make down` is run, the `minio-storage` named volume is wiped and both buckets are lost — they must be recreated before the next run.

### Step 3: Start the producer

The producer runs inside the Spark container alongside the aggregator — both use the same `boto3`/`pyspark` image, and the container already has `KINESIS_ENDPOINT=http://etl-project-2-localstack:4566` wired up via `docker-compose.yml`.

```bash
make producer
```

Which expands to:

```bash
docker exec etl-project-2-spark python /opt/spark/scripts/kinesis_stream_producer.py \
  --stream-name music-streams \
  --batch-size 20 \
  --interval-seconds 5.0 \
  --local
```

The producer auto-creates the `music-streams` Kinesis stream on first run, then sends batches of 20 events every 5 seconds continuously. Stop it with Ctrl+C when you're done.

### Step 4: Start the aggregator

The aggregator also runs inside the Spark container. Use `make consumer` for the shorthand, or run it directly:

```bash
docker exec -d etl-project-2-spark spark-submit \
  --jars /opt/spark/jars/spark-streaming-sql-kinesis-connector_2.12-1.0.0.jar,/opt/spark/jars/hadoop-aws-3.3.4.jar,/opt/spark/jars/aws-java-sdk-bundle-1.12.565.jar \
  /opt/spark/scripts/spark_aggregator.py \
    --kinesis-stream music-streams \
    --songs-path s3a://etl-data/songs.csv \
    --users-path s3a://etl-data/users.csv \
    --output-path s3a://aggregations/ \
    --window-minutes 5 \
    --watermark-minutes 1 \
    --local

# Follow the logs to confirm all 3 streaming queries started
docker exec etl-project-2-spark tail -f /tmp/consumer.log
```

> **First-run behaviour — `TRIM_HORIZON`:** The Kinesis read is configured with `kinesis.initialPosition = TRIM_HORIZON`. This means Spark reads **from the very beginning of the shard** when the consumer starts — not just from the current tip of the stream. If the producer has been running for several minutes before `make consumer` is executed, Spark will replay all previously published records in the first few micro-batches and then catch up to live data automatically. No events are lost regardless of start order. Because Spark uses **event time** (the `timestamp` embedded in each record) for windowing rather than processing time, backlog records are bucketed into their original windows and windows close correctly once the backlog is consumed.

### Step 5: Verify output

After about 5 minutes (one window closes), open **http://localhost:9001** → `aggregations` bucket. Three partition folders should appear:

```
aggregations/
├── hourly_streams/window_start=.../
├── top_tracks_hourly/window_start=.../
└── country_metrics_hourly/window_start=.../
```

Each partition contains a single compacted Parquet file — the `foreachBatch` + `coalesce` handler takes care of that so I don't end up with hundreds of 2KB fragments.

### Step 6: Tear down

```bash
# Kill the running pipeline processes inside the container
docker exec etl-project-2-spark sh -c \
  'kill $(pgrep -f spark-submit) $(pgrep -f kinesis_stream_producer) 2>/dev/null; echo done'

# Stop the containers and wipe volumes
make down
```

---

## A Few Design Choices Worth Noting

**Broadcast joins** — songs and users are static reference data that fits comfortably in memory, so I broadcast them. This avoids any shuffle on the streaming side and keeps throughput linear with event volume.

**`foreachBatch` + `coalesce` for file compaction** — Spark's default behaviour on streaming writes produces hundreds of tiny files per micro-batch. I use `foreachBatch` to intercept each batch, calculate the right number of output files targeting ~5MB each, and `coalesce` before writing. In practice this gives one file per window partition.

**`--local` flag for environment routing** — the same `spark_aggregator.py` runs locally and in production. The `--local` flag switches endpoint URLs from LocalStack/MinIO to real AWS. No environment-specific code paths.

**Configurable window and watermark** — defaults are 5-min window / 1-min watermark, which gives a reasonable feedback loop locally while being realistic enough to test late-data handling. For production I'd run `--window-minutes 60 --watermark-minutes 15`.

---

## Makefile Reference

```bash
make up        # Start all 3 containers
make down      # Stop containers and remove volumes
make logs      # Tail docker-compose logs
make test      # Run unit tests
make producer  # Run the Kinesis producer inside the Spark container
make consumer  # Run the Spark aggregator inside the Spark container (writes to s3a://aggregations/)
```

Both `make producer` and `make consumer` run inside `etl-project-2-spark` via `docker exec` — the whole pipeline is self-contained within Docker.

---

## Comparing with ETL-Project-1

| | Project-1 | Project-2 |
|---|---|---|
| **Source** | CSV files in S3 | Streaming events via Kinesis |
| **Trigger** | Scheduled Airflow DAG | Continuous micro-batches |
| **Framework** | Airflow + PySpark | PySpark Structured Streaming |
| **Output** | Redshift | S3 + Athena |
| **Local stack** | Airflow in Docker | Spark + LocalStack + MinIO |

---

**Built with**: PySpark 3.5 · LocalStack · MinIO · Docker  
**Region**: `ap-south-2`  
**Status**: Fully tested locally, ready for AWS deployment
