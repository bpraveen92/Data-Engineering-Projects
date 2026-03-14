# ETL-Project-3: Trip Lifecycle Streaming Pipeline

I built this project to model a pretty common real-world pattern: two independent event streams (`trip_start` and `trip_end`) that eventually need to become one reliable analytical dataset.
After about 8 years building data pipelines, this is the exact shape I prefer when I need clean state transitions plus analytics-ready output.

At a high level, this is what I’m doing:

1. I ingest `trip_start` events into DynamoDB.
2. I ingest `trip_end` events and update the same trip records.
3. I trigger a Glue PySpark job after end events land.
4. I write hourly aggregated parquet to S3.
5. I expose that output through Glue Crawler + Athena.

## Pipeline Architecture

```text
trip_start.csv ------------------------> Kinesis: trip-start-stream -----> Lambda trip_start -----> DynamoDB trip_lifecycle
                                                                                 (insert/upsert)

trip_end.csv --------------------------> Kinesis: trip-end-stream -------> Lambda trip_end -------> DynamoDB trip_lifecycle
                                                                                 (update/upsert)           |
                                                                                                            +--> Glue StartJobRun
                                                                                                                     |
                                                                                                                     v
                                                                                                       Glue PySpark aggregation
                                                                                                                     |
                                                                                                                     v
                                                                                                           S3 parquet (partitioned)
                                                                                                                     |
                                                                                                               Glue Crawler
                                                                                                                     |
                                                                                                                    Athena
```

## Core Behavior I’m Enforcing

- In `trip_start` Lambda, I do deterministic updates keyed by `trip_id`, so duplicate start events stay idempotent.
- In `trip_end` Lambda, I update existing trips, but if start is missing I upsert and mark `data_quality=missing_start`.
- In Glue, I aggregate only completed and valid records (`trip_status=completed` and not `missing_start`).
- I aggregate at hourly grain using `event_hour = date_trunc('hour', end_dropoff_datetime)`.
- I can also compute top routes per hour using a Spark window ranking (`row_number` over each hour).

## Code Flow Walkthrough

When I run this project, this is the exact code path:

1. `scripts/setup_local.py`
- Entry: `main()`
- Core methods: `ensure_stream_active`, `ensure_table_exists`, `ensure_bucket_exists`, `ensure_glue_database_exists`
- Outcome: local infra prerequisites are ready.

2. `scripts/kinesis_trip_producer.py`
- Entry: `main()`
- Core methods: `read_rows`, `publish_interleaved`, `publish_batch`
- Outcome: trip start and trip end events are pushed to two streams.

3. `scripts/local_pipeline_runner.py`
- Entry: `main()`
- Core methods: `fetch_stream_records`, `run_in_batches`, `summarize_batch_output`
- Outcome: stream records are pulled and sent to Lambda handlers in batches.

4. `scripts/lambda_trip_start.py`
- Entry: `handler(event, context)`
- Core methods: `decode_kinesis_record`, `validate_start_event`, `write_trip_start`
- Outcome: trip start state is written into DynamoDB.

5. `scripts/lambda_trip_end.py`
- Entry: `handler(event, context)`
- Core methods: `decode_kinesis_record`, `validate_end_event`, `write_trip_end`, `start_glue_job`
- Outcome: trip rows are completed and Glue is triggered (AWS mode).

6. `scripts/glue_trip_aggregator.py`
- Entry: `main()`
- Core methods: `run_local_scan_aggregation` or `run_glue_aggregation`, then `transform_completed_trips`, `transform_top_routes_per_hour`, `write_output`
- Outcome: hourly parquet metrics are written to output storage.

## Project Structure

```text
ETL-Project-3/
├── Dockerfile
├── docker-compose.yml
├── .dockerignore
├── data/
│   ├── trip_start.csv
│   └── trip_end.csv
├── docs/
│   ├── EXECUTION.md
│   ├── LOCAL_DEVELOPMENT_SETUP.md
│   ├── AWS_DEPLOYMENT.md
│   └── TROUBLESHOOTING.md
├── scripts/
│   ├── lambda_trip_start.py
│   ├── lambda_trip_end.py
│   ├── glue_trip_aggregator.py
│   ├── kinesis_trip_producer.py
│   ├── setup_local.py
│   ├── local_pipeline_runner.py
│   └── iam/
├── tests/
│   ├── test_lambda_trip_start.py
│   ├── test_lambda_trip_end.py
│   └── test_glue_trip_aggregator.py
├── artifacts/
│   ├── lambda/
│   └── sql/athena_queries.sql
├── Makefile
├── requirements.txt
└── .env.example
```

## Quick Start (Local Simulation)

I usually test this inside Docker first before I touch AWS.

1. Start the local stack:

```bash
make up
```

2. Run the full containerized flow:

```bash
make docker-all
```

That runs setup, producer, Lambda simulation, and aggregation from the app container.

3. Check output files:

```bash
find output/aggregations -type f | sort
```

Optional: if I want top routes per hour as a second analytical output, I set env vars before running:

```bash
export TOP_ROUTES_OUTPUT_PATH=./output/top_routes_hourly
export TOP_ROUTES_LIMIT=5
make docker-runner
```

4. Stop the stack when done:

```bash
make down
```

If I need to run directly from host Python instead of Docker, I do this:

```bash
cp .env.example .env
make install
make local-all
```

## AWS Deployment Style

For this project, I’m intentionally keeping AWS setup manual via Console (not full automation).
I do not create Lambda resources through code; I create them manually in the AWS UI.
I also keep Lambda deployment lightweight by using the code editor with the `.py` source from `scripts/lambda_trip_start.py` and `scripts/lambda_trip_end.py` (no zip packaging step).

```bash
cp .env.example .env
# update .env with your real region and names
```

Then I follow the checklist in [`docs/AWS_DEPLOYMENT.md`](docs/AWS_DEPLOYMENT.md).

## Testing

```bash
make test
```

Tests cover Lambda validation/update semantics and the aggregation logic.

## Docs

- [`docs/EXECUTION.md`](docs/EXECUTION.md): my runbook for local + AWS execution.
- [`docs/LOCAL_DEVELOPMENT_SETUP.md`](docs/LOCAL_DEVELOPMENT_SETUP.md): local endpoints and setup notes.
- [`docs/AWS_DEPLOYMENT.md`](docs/AWS_DEPLOYMENT.md): full manual AWS Console prerequisites and setup.
- [`docs/TROUBLESHOOTING.md`](docs/TROUBLESHOOTING.md): issues I expect and how I handle them.
