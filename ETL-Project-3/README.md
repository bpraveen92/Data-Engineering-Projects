# ETL-Project-3: Trip Lifecycle Streaming Pipeline

I built this to model a real-world problem I kept running into: two independent event streams that represent the start and end of the same thing — a taxi trip — arriving at different times, out of order, and needing to be reliably joined before any analytics can happen.

**The dataset** is a synthetic NYC-style ride-sharing dataset — 4,999 trip start events and 4,999 trip end events spread across a single day (2024-05-25). `trip_start` events carry the pickup location, estimated dropoff zone, and estimated fare. `trip_end` events carry the actual dropoff time, distance, fare, and tip. The two sides share only a `trip_id`.

**What I'm trying to simulate here -** is a ride-sharing platform where trip lifecycle events are produced by two separate backend services — a dispatch service fires `trip_start` when a driver accepts a ride, and a billing service fires `trip_end` when the ride closes out. In production these would be independent Kinesis producers with no coordination between them. Events arrive out of order: a `trip_end` can arrive before its `trip_start` (driver app lag, network retry), and the gap between the two events can range from minutes to hours. The pipeline has to handle all of this without dropping trips or double-counting them in the hourly aggregates.

The pipeline ingests these events from separate Kinesis streams, joins them through DynamoDB, stages completed trips in S3, and runs hourly Glue aggregations to produce partitioned Parquet output queryable through Athena.

---

## Architecture

```
trip_start.csv ──► trip-start-stream ──► Lambda (trip_start) ─┐
                                                                ├──► DynamoDB: trip_lifecycle
trip_end.csv ───► trip-end-stream ────► Lambda (trip_end) ────┘         │
                                                               S3: staging/completed_trips/
                                                               (partitioned by dropoff hour)
                                                                         │
                                                               EventBridge (hourly schedule)
                                                                         │
                                                               Glue: glue_trip_aggregator
                                                               (incremental, checkpoint-aware)
                                                                         │
                                                               S3: aggregations/hourly_zone_metrics/
                                                                         │
                                                               Glue Crawler ──► Athena
```

---

## How Data Flows — A Real Example

Here's a single trip moving through every stage.

**Input CSVs:**
```
# trip_start.csv
trip_id,     pickup_location_id, dropoff_location_id, pickup_datetime,       estimated_fare_amount
c66ce556bc,  93,                 132,                 2024-05-25 13:19:00,   34.19

# trip_end.csv
trip_id,     dropoff_datetime,      fare_amount, tip_amount, trip_distance
c66ce556bc,  2024-05-25 14:05:00,   40.10,       0.0,        0.1
```

**After `lambda_trip_start` writes to DynamoDB** (`trip_status=started`, start-side fields only):
```json
{
  "trip_id": "c66ce556bc",
  "start_pickup_location_id": 93,
  "start_dropoff_location_id": 132,
  "start_pickup_datetime": "2024-05-25 13:19:00",
  "trip_status": "started"
}
```

**After `lambda_trip_end` updates the same row** (merges end-side fields, marks completed):
```json
{
  "trip_id": "c66ce556bc",
  "start_pickup_location_id": 93,
  "start_dropoff_location_id": 132,
  "end_dropoff_datetime": "2024-05-25 14:05:00",
  "end_fare_amount": 40.10,
  "end_tip_amount": 0.0,
  "end_trip_distance": 0.1,
  "trip_status": "completed",
  "data_quality": "ok"
}
```

**S3 staging file written by `lambda_trip_end`** (compact — only what Glue needs):
```
s3://<bucket>/staging/completed_trips/year=2024/month=05/day=25/hour=14/completed_trip_batch_20240525T140500Z_<uuid>.jsonl
```
```json
{
  "trip_id": "c66ce556bc",
  "pickup_location_id": 93,
  "dropoff_location_id": 132,
  "dropoff_datetime": "2024-05-25 14:05:00",
  "fare_amount": 40.10,
  "tip_amount": 0.0,
  "trip_distance": 0.1,
  "trip_status": "completed",
  "data_quality": "ok"
}
```

**Glue output** (hourly Parquet, aggregated across all trips in that hour+route):
```
s3://<bucket>/aggregations/hourly_zone_metrics/year=2024/month=05/day=25/hour=14/part-0.parquet
```
```
event_hour           pickup_location_id  dropoff_location_id  trip_count  fare_sum  tip_sum  distance_sum
2024-05-25 14:00:00  93                  132                  17          480.13    12.50    31.4
```

---

## Key Decisions I Made (and Changed)

**Why DynamoDB for state, not a database join in Spark?**
The two streams arrive independently and out of order. There's no guarantee `trip_start` comes before `trip_end`. I needed a fast, key-value store to hold partial state — DynamoDB with `trip_id` as the partition key is the natural fit. Each Lambda just reads the existing item and upserts.

**Why not have Glue scan DynamoDB directly?**
My first version did this. The problem is that every Glue run would do a full DynamoDB table scan — expensive, slow, and it grows unboundedly. I switched to having `lambda_trip_end` write compact completed-trip events directly to S3 staging partitioned by `dropoff_datetime` hour. Now Glue only reads the new files it hasn't seen yet.

**Why stage by `dropoff_datetime` hour, not Lambda invocation time?**
The S3 partition key is derived from the trip's `dropoff_datetime`, not from when Lambda ran. A trip with `dropoff_datetime=13:45` that Lambda processes at 14:25 lands in `staging/.../hour=13/`, not `hour=14/`. This keeps the staging partition aligned with the output partition — Glue groups by `date_trunc('hour', dropoff_datetime)`, so a late-arriving trip always folds into the correct hour aggregate regardless of when Lambda wrote its file.

**Why a checkpoint instead of reprocessing everything?**
I keep a checkpoint JSON in S3 that tracks the `last_modified` timestamp and S3 key of the last staged file each Glue run processed. On the next run I skip everything at or before that marker. But there's a scalability problem with naively listing the full `staging/completed_trips/` prefix — after months of data that becomes thousands of files to page through, and almost all of them are behind the checkpoint. So on incremental runs I narrow the S3 listing to only the 2 most recently closed hour prefixes: `floor(now) - 2h` and `floor(now) - 1h`. The currently-open hour is excluded — it has only a few minutes of data and will be fully captured on the next run. At 15:05 that's just `hour=13` and `hour=14`. The checkpoint filter then runs on that small set. Combined with `spark.sql.sources.partitionOverwriteMode=dynamic`, only the affected output partitions get rewritten.

**How I handle late arrivals**
Lateness is enforced by the listing scope, not by a per-file age check. On incremental runs, Glue only lists the 2 most recently closed hour prefixes. Any new file that lands in those 2 prefixes gets processed — it doesn't matter whether the hour closed 5 minutes or 65 minutes ago. If a late-arriving event's staging file falls outside those 2 prefixes, it simply isn't listed and never reaches aggregation.

Example at a 15:05 run: Glue lists `hour=13` and `hour=14`. A trip with `dropoff_datetime=13:47` that Lambda processed at 14:55 wrote its file to `staging/.../hour=13/` — it's in the window, it gets counted. The same trip arriving at 16:15 misses the 16:05 run entirely because `hour=13` is no longer in the listing window at that point.

**Why not trigger Glue from Lambda?**
I tried this. The problem is Lambda gets invoked per batch of Kinesis records — potentially dozens of times a minute during high traffic. Glue startup alone takes 1-2 minutes, so triggering it from Lambda creates a queue of competing Glue runs fighting over the same checkpoint. I moved Glue to an hourly EventBridge schedule instead. Lambda's only job now is: write to DynamoDB, write to S3 staging, done.

**Why two staging read paths in Glue?**
`local_staging` uses boto3 to read staged files from LocalStack S3 — simple, no `s3a` config needed, easy to debug in Docker. `glue_staging` uses Glue's native Spark S3 reader in AWS. Both paths share the same `transform_completed_trips` Spark logic after the read.

---

## Missing Start Handling

If `trip_end` arrives before `trip_start` (which happens regularly), I still write the trip to DynamoDB — I just mark it `data_quality=missing_start`. The staging write to S3 is skipped in this case, so these trips never reach Glue aggregation. They stay in DynamoDB as a quality signal.

---

## Quick Start (Local)

```bash
make up               # start Docker stack (LocalStack + app container)
make docker-all       # setup → produce → process → aggregate (one command)
make down             # stop and clean
```

Check output:
```bash
find output/aggregations -type f | sort
```

Optional top-routes analytical output:
```bash
export TOP_ROUTES_OUTPUT_PATH=./output/top_routes_hourly
export TOP_ROUTES_LIMIT=5
make docker-runner
```

---

## Docker Setup

Two containers, one network.

**`etl-project-3-localstack`** runs [LocalStack](https://github.com/localstack/localstack) — an AWS emulator that provides in-process Kinesis, DynamoDB, S3, and Glue endpoints at `http://localhost:4566`. `make up` starts this container first. The app container won't start until LocalStack passes its healthcheck (`curl http://localhost:4566/_localstack/health`), which is enforced by `depends_on: condition: service_healthy` in compose.

**`etl-project-3-app`** is the Python/PySpark runtime. It's built from the project Dockerfile and mounts the entire project directory as `/workspace` — so code changes are visible inside the container immediately without rebuilding. All `make docker-*` targets run scripts inside this container via `docker exec`.

**Dockerfile line by line:**

```dockerfile
FROM python:3.11-slim-bookworm
```
Slim Debian-based image. No GUI tools, no compilers, no package bloat — just enough OS to run Python.

```dockerfile
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1
```
`PYTHONDONTWRITEBYTECODE` stops Python from writing `.pyc` files into the mounted workspace (those would show up as uncommitted clutter in the project directory). `PYTHONUNBUFFERED` flushes stdout/stderr immediately — without this, `log.info()` output would only appear after the script finishes, which makes debugging much harder inside Docker.

```dockerfile
RUN apt-get install -y --no-install-recommends \
    curl \
    openjdk-17-jre-headless
```
Two system dependencies: `curl` is used by the LocalStack healthcheck; `openjdk-17-jre-headless` is required by PySpark — Spark runs on the JVM and won't start without a Java runtime. The `--no-install-recommends` flag skips suggested packages (documentation, optional extensions) that would bloat the image.

```dockerfile
COPY requirements.txt /tmp/requirements.txt
RUN python3 -m pip install --no-cache-dir --retries 10 --default-timeout 300 -r /tmp/requirements.txt
```
`requirements.txt` is copied before the rest of the project so Docker can cache the pip install layer. As long as `requirements.txt` doesn't change, rebuilds skip this slow step entirely. `--no-cache-dir` keeps the image lean by not storing the pip download cache inside the image. `--retries 10 --default-timeout 300` handles flaky network during build (PySpark is a large download and occasionally times out on slower connections).

```dockerfile
CMD ["tail", "-f", "/dev/null"]
```
Keeps the container running indefinitely without doing anything. All actual work is triggered externally via `docker exec` from Makefile targets — the container is just an execution environment that stays alive between commands.

**Why the workspace volume mount instead of `COPY . /workspace`?**
A `COPY` would bake the source files into the image at build time — every code change would require a full rebuild. The volume mount (`./:/workspace`) means the container always sees the current state of the files on disk. This is the standard pattern for development containers.

**Credentials in docker-compose:**
```yaml
AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
```
LocalStack doesn't validate credentials — it accepts any non-empty string. `test/test` is the conventional placeholder. The same dummy values are set in both containers so boto3 doesn't complain about missing credentials.

---

## Project Structure

```
ETL-Project-3/
├── data/
│   ├── trip_start.csv              # 4999 sample trip start events
│   └── trip_end.csv                # 4999 sample trip end events
├── scripts/
│   ├── setup_local.py              # creates LocalStack resources
│   ├── kinesis_trip_producer.py    # publishes CSVs to Kinesis streams
│   ├── local_pipeline_runner.py    # simulates Lambda + Glue locally
│   ├── lambda_trip_start.py        # Kinesis → DynamoDB (start side)
│   ├── lambda_trip_end.py          # Kinesis → DynamoDB + S3 staging (end side)
│   ├── glue_trip_aggregator.py     # incremental PySpark aggregator
│   └── iam/                        # trust policy JSON files for IAM setup
├── tests/
│   ├── test_lambda_trip_start.py
│   ├── test_lambda_trip_end.py
│   └── test_glue_trip_aggregator.py
├── artifacts/
│   └── sql/athena_queries.sql      # ready-to-run Athena queries
├── docs/
│   ├── EXECUTION.md                # step-by-step runbook (local + AWS)
│   ├── LOCAL_DEVELOPMENT_SETUP.md  # Docker setup, endpoints, debug tips
│   ├── AWS_DEPLOYMENT.md           # manual Console setup checklist
│   └── TROUBLESHOOTING.md          # common issues and fixes
├── Makefile
├── docker-compose.yml
├── requirements.txt
└── .env.example
```

---

## Testing

```bash
make test
```

21 tests covering Lambda validation, DynamoDB update semantics, deduplication, late arrival filtering, checkpoint logic, Glue aggregation output, and end-to-end incremental run validation.

`tests/test_incremental_aggregation.py` uses [moto](https://github.com/getmoto/moto) to mock S3 in-memory — no LocalStack or real AWS needed. The `@mock_aws` decorator (moto v5+) intercepts all boto3 S3 calls within the test and tears down the mock when the test exits. These two tests verify the core late arrival contract: a file landing in an hour prefix within the 2-hour listing window is picked up and re-aggregated; a file in an older prefix is never listed.

---

## AWS Deployment

I do AWS setup manually in the Console for this project — no IaC. The full checklist is in [`docs/AWS_DEPLOYMENT.md`](docs/AWS_DEPLOYMENT.md).
