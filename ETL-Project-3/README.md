# ETL-Project-3: Trip Lifecycle Streaming Pipeline

I built this project to model a pretty common real-world pattern: two independent event streams (`trip_start` and `trip_end`) that eventually need to become one reliable analytical dataset.
After about 8 years building data pipelines, this is the exact shape I prefer when I need clean state transitions plus analytics-ready output.

At a high level, this is what I’m doing:

1. I ingest `trip_start` events into DynamoDB.
2. I ingest `trip_end` events and update the same trip records.
3. I write compact completed-trip events from `trip_end` into an append-only S3 staging area.
4. I run Glue on a schedule and aggregate only the newly affected staged hours.
5. I expose the curated parquet output through Glue Crawler + Athena.

## Pipeline Architecture

```text
trip_start.csv ------------------------> Kinesis: trip-start-stream -----> Lambda trip_start -----> DynamoDB trip_lifecycle
                                                                                 (insert/upsert)

trip_end.csv --------------------------> Kinesis: trip-end-stream -------> Lambda trip_end -------> DynamoDB trip_lifecycle
                                                                                 (update/upsert)           |
                                                                                                           +--> S3 staging/completed_trips
                                                                                                                     |
                                                                                                      EventBridge schedule / manual Glue run
                                                                                                                     |
                                                                                                                     v
                                                                                                    Glue incremental PySpark aggregation
                                                                                                                     |
                                                                                                                     v
                                                                                                           S3 parquet (partitioned)
                                                                                                                     |
                                                                                                               Glue Crawler
                                                                                                                     |
                                                                                                                    Athena
```

## Trigger Model I Recommend

For AWS deployment, I recommend native Kinesis -> Lambda event source mappings instead of EventBridge schedules.

- Lambda is invoked only when records are available in the stream.
- AWS manages shard polling, checkpoints, and retry behavior for us.
- When the stream is quiet, there is nothing for us to poll manually, so the function is effectively idle.

I do not recommend scheduled EventBridge polling for this project because it adds extra lag, manual checkpoint logic, and more moving parts without giving us a real benefit here.

## Core Behavior I’m Enforcing

- In `trip_start` Lambda, I do deterministic updates keyed by `trip_id`, so duplicate start events stay idempotent.
- In `trip_end` Lambda, I update existing trips, but if start is missing I upsert and mark `data_quality=missing_start`.
- In `trip_end` Lambda, I also emit a compact analytics record to `staging/completed_trips/...` only when the trip is valid and complete.
- In Glue, I aggregate only completed and valid staged records (`trip_status=completed` and not `missing_start`).
- I aggregate at hourly grain using `event_hour = date_trunc('hour', dropoff_datetime)`.
- I can also compute top routes per hour using a Spark window ranking (`row_number` over each hour).
- I keep a checkpoint in S3 so every Glue run only looks at newly arrived staged files, then rewrites only the affected hourly partitions.

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
- Core methods: `decode_kinesis_record`, `validate_end_event`, `write_trip_end`, `build_completed_trip_event`, `write_staging_records`
- Outcome: trip rows are completed in DynamoDB and valid completed trips are appended to staged S3 files.

6. `scripts/glue_trip_aggregator.py`
- Entry: `main()`
- Core methods: `run_local_staging_aggregation` or `run_glue_staging_aggregation`, then `transform_completed_trips`, `transform_top_routes_per_hour`, `write_output`
- Outcome: only newly affected hourly parquet partitions are recalculated and written to output storage.

## Why I Use Two Staging Read Paths

I intentionally keep two staged-file read strategies in `glue_trip_aggregator.py`:

1. `local_staging` (boto3 S3 reads + Spark DataFrame)
- I use this for local Docker testing.
- It works cleanly with LocalStack and does not require `s3a` or Glue runtime-specific extras.
- It keeps local debugging simple because I can inspect staged JSONL rows before Spark transforms.

2. `glue_staging` (Glue Spark reads from S3 prefixes)
- I use this in AWS Glue runtime for production-style execution.
- Glue can read the staged S3 prefixes directly and then overwrite only the affected output partitions.
- It avoids rescanning the operational DynamoDB table every time the job runs.

The reason for two paths is practical: local reliability and faster iteration on one side, AWS-native S3 reads on the other side, while reusing the same Spark transformation logic after read.

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

## Daily Runtime Expectations

On a normal day in AWS, this is how I expect the pipeline to behave:

1. New trip records land in Kinesis.
2. AWS invokes the matching Lambda through the Kinesis event source mapping.
3. Lambda writes or updates trip state in DynamoDB.
4. `trip_end` Lambda appends compact completed-trip rows into staged S3 files.
5. Glue runs on a schedule, reads only newly discovered staged files, and rewrites the affected hourly partitions.
6. Crawlers refresh Athena-facing metadata.

The main limitation I keep in mind is that Kinesis-triggered Lambda scales primarily with shard count, so one small shard can become the bottleneck. On the analytics side, this design is much cheaper than full-table DynamoDB recomputes, but it still depends on partition overwrite patterns and checkpoint correctness, so I monitor the staging prefix and checkpoint object closely.

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
