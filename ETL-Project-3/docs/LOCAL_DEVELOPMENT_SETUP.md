# Local Development Setup

Everything I need to know to get the local Docker environment running cleanly and debug it when something goes wrong.

---

## What Runs Locally

| Component | Local Equivalent | Tool |
|---|---|---|
| Kinesis streams | Emulated Kinesis | LocalStack |
| DynamoDB table | Emulated DynamoDB | LocalStack |
| S3 bucket | Emulated S3 | LocalStack |
| Lambda functions | Python function calls | `local_pipeline_runner.py` |
| Glue job | `run_local_staging_aggregation()` | Direct PySpark in app container |

One important difference: locally I call the Lambda handlers directly as Python functions — there's no real event source mapping. The `local_pipeline_runner.py` script pulls records from the LocalStack Kinesis streams and hands them to `lambda_trip_start.handler` and `lambda_trip_end.handler` in batches, simulating what AWS would do automatically.

---

## Endpoints

| Service | Inside Docker network | From host |
|---|---|---|
| LocalStack (all services) | `http://etl-project-3-localstack:4566` | `http://localhost:4566` |
| App container hostname | `etl-project-3-app` | N/A |

All scripts pick up the endpoint from `AWS_ENDPOINT_URL` or service-specific env vars (`DYNAMODB_ENDPOINT_URL`, `S3_ENDPOINT_URL`). These are pre-set in `docker-compose.yml` for the app container.

---

## Environment Variables

Copy `.env.example` to `.env` and fill in values for host-mode runs. For Docker runs, env vars are already set in `docker-compose.yml` — you don't need `.env` unless you're running scripts directly on your host.

Key variables:

| Variable | Local value | Purpose |
|---|---|---|
| `AWS_ENDPOINT_URL` | `http://etl-project-3-localstack:4566` | Routes all boto3 calls to LocalStack |
| `AWS_REGION` | `ap-south-2` | Must match what setup_local.py creates |
| `TRIP_LIFECYCLE_TABLE` | `trip_lifecycle` | DynamoDB table name |
| `AGGREGATION_BUCKET` | `etl-project-3-analytics-local` | S3 bucket for staging + output |
| `COMPLETED_TRIP_STAGING_PREFIX` | `staging/completed_trips` | Where Lambda writes JSONL |
| `AGGREGATION_CHECKPOINT_URI` | `s3://etl-project-3-analytics-local/checkpoints/hourly_zone_metrics_checkpoint.json` | Glue incremental checkpoint |

---

## Docker Workflow

```bash
make up                    # start LocalStack + app container
make docker-setup-local    # create streams, table, bucket in LocalStack
make docker-producer       # publish trip_start.csv + trip_end.csv to Kinesis
make docker-runner         # process streams → DynamoDB → S3 staging → Parquet
make down                  # stop and remove containers
```

Or all at once:
```bash
make up && make docker-all && make down
```

---

## What Each Script Does

**`setup_local.py`**
Creates the LocalStack resources: two Kinesis streams, DynamoDB table, S3 bucket. Safe to re-run — it checks for existing resources before creating. The Glue database warning is expected (not available in LocalStack community edition).

**`kinesis_trip_producer.py`**
Reads `data/trip_start.csv` and `data/trip_end.csv`, then publishes both to their respective Kinesis streams in interleaved batches. Interleaving is intentional — it simulates real traffic where start and end events are mixed on the wire.

**`local_pipeline_runner.py`**
The local orchestrator. It:
1. Reads all records from both Kinesis streams via `GetShardIterator` + `GetRecords`
2. Calls `lambda_trip_start.handler` with batches of start records
3. Calls `lambda_trip_end.handler` with batches of end records
4. Calls `run_local_staging_aggregation` from `glue_trip_aggregator.py`

This is the only script you need to run after the producer. It handles everything Lambda and Glue would do in AWS.

---

## Validating the Run

### 1. Runner console output

The most immediate signal is the summary printed by `make docker-runner`. Look for these lines at the end:

```
INFO  trip_start summary={'processed': 4999, 'failed': 0}
INFO  trip_end summary={'processed': 4468, 'failed': 531, 'staged_completed_trips': 4468}
INFO  Local incremental aggregation completed summary={
        'metric_rows': 3460,
        'affected_hours': 25,
        'new_objects': 1145,
        'top_route_rows': 0
      }
```

`failed=531` on trip_end is expected — those rows in `trip_end.csv` have missing required fields. `staged_completed_trips=4468` is the number of completed trips that passed validation and were written to S3 staging. `affected_hours=25` is how many hour partitions Glue wrote to Parquet.

---

### 2. Parquet output — browse in your file explorer

The `output/` directory is mounted from the container to your host, so you can open it directly in Finder or your IDE's file explorer after the run.

You should see 25 `hour=XX` folders spanning 2024-05-25 hours 00–23 plus one spillover into 2024-05-26 hour=00:

```
output/
└── aggregations/
    └── year=2024/
        └── month=05/
            ├── day=25/
            │   ├── hour=00/part-0.parquet
            │   ├── hour=01/part-0.parquet
            │   │   ...
            │   └── hour=23/part-0.parquet
            └── day=26/
                └── hour=00/part-0.parquet   ← late drop-offs from the previous day
```

If the folder is empty or fewer than 25 partitions exist, the aggregation step didn't complete — check the runner logs for errors.

---

### 3. Reading a Parquet file manually

Open a Python shell from the project root and read any partition directly to inspect the aggregated rows:

```python
import pandas as pd

df = pd.read_parquet("output/aggregations/year=2024/month=05/day=25/hour=14/part-0.parquet")
print(df.head())
print(df.dtypes)
```

A healthy partition looks like this — one row per unique pickup/dropoff route in that hour:

```
event_hour           pickup_location_id  dropoff_location_id  trip_count  fare_sum   tip_sum  distance_sum
2024-05-25 14:00:00  4                   4                    1           17.76      0.0      2.3
2024-05-25 14:00:00  7                   179                  1           14.10      0.0      5.02
2024-05-25 14:00:00  13                  246                  1           36.47      0.0      3.1
...
```

You can also check `df["trip_count"].sum()` across all partitions — it should add up to 4468 (the number of staged completed trips).

---

### 4. Checkpoint file

The checkpoint JSON is written to LocalStack S3. Its purpose is to track which staged files Glue has already processed so incremental runs don't re-aggregate the same data. After the first run it should look like this:

```json
{
  "last_modified": "2024-05-25T14:30:05+00:00",
  "last_key": "staging/completed_trips/year=2024/month=05/day=25/hour=14/completed_trip_batch_20240525T143005Z_<uuid>.jsonl",
  "processed_object_count": 1145
}
```

`processed_object_count` reflects the number of JSONL files Lambda wrote to staging — one file per invocation batch. If you run `make docker-runner` a second time without republishing new events, Glue will find no new objects past the checkpoint and exit cleanly with `new_objects=0`.

---

## Running Tests

```bash
make test
```

Or directly:
```bash
python3 -m pytest tests/ -v
```

Tests run on host Python (not inside Docker) and mock out all AWS calls, so they're fast and don't need LocalStack running.

---

## Host-mode (no Docker)

If I want to skip Docker and run everything directly on my laptop:

```bash
cp .env.example .env
# update .env: set AWS_ENDPOINT_URL=http://localhost:4566 and other values
make install
make local-all
```

This still requires LocalStack running somewhere accessible at `localhost:4566`. I usually start it separately with `make up` and then run scripts locally against it.
