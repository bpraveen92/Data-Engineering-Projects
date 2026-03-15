# Local Development Setup

This is how I run the project locally when I want fast feedback before touching AWS.

I run local testing in Docker by default. That keeps my laptop environment clean and gives me repeatable runs.

## Endpoints I Use

| Service | Local URL |
|---|---|
| App container | `etl-project-3-app` |
| LocalStack edge (inside Docker network) | `http://etl-project-3-localstack:4566` |
| LocalStack edge (host access) | `http://localhost:4566` |
| Kinesis API | `http://localhost:4566` |
| DynamoDB API | `http://localhost:4566` |
| S3 API | `http://localhost:4566` |

## Docker Workflow I Use

I keep these rough time expectations in mind:

| Command | Typical Time |
|---|---|
| `make up` | 30 sec to 90 sec (first build can take 5 to 8 min) |
| `make docker-setup-local` | 20 sec to 40 sec |
| `make docker-producer` | 60 sec to 120 sec |
| `make docker-runner` | 6 min to 10 min |
| `make down` | 10 sec to 30 sec |

1. Start stack:
```bash
make up
```

2. Create local resources in LocalStack:
```bash
make docker-setup-local
```

3. Publish both CSV streams:
```bash
make docker-producer
```

4. Process streams and aggregate:
```bash
make docker-runner
```

5. Stop and clean:
```bash
make down
```

## Terminal Signals I Watch For

When I run local Docker commands, these are the key lines I expect to see:

1. `make up`
```text
Container etl-project-3-localstack Started
Container etl-project-3-localstack Healthy
Container etl-project-3-app Started
```

2. `make docker-setup-local`
```text
INFO Creating Kinesis stream: trip-start-stream
INFO Creating Kinesis stream: trip-end-stream
INFO Creating DynamoDB table: trip_lifecycle
INFO Local setup complete.
```

3. `make docker-producer`
```text
INFO Starting publish with start_rows=4999 end_rows=4999
INFO trip_start -> trip-start-stream sent=100 total=100
INFO trip_end -> trip-end-stream sent=100 total=100
INFO Publishing complete. start=4999 end=4999
```

4. `make docker-runner`
```text
INFO Fetched stream records: trip_start=4999 trip_end=4999
INFO trip_start summary={'processed': 4999, 'failed': 0, 'staged_completed_trips': 0}
INFO trip_end summary={'processed': 4468, 'failed': 531, 'staged_completed_trips': 4468}
INFO Local incremental aggregation completed summary={'metric_rows': 3460, 'affected_hours': 25, 'top_route_rows': 0, 'new_objects': 1145}
```

If I want to generate an additional analytical dataset for top routes per hour:

```bash
export TOP_ROUTES_OUTPUT_PATH=./output/top_routes_hourly
export TOP_ROUTES_LIMIT=5
make docker-runner
```

`make docker-runner` now forwards `TOP_ROUTES_OUTPUT_PATH` and `TOP_ROUTES_LIMIT` into the app container, so this works directly from host shell exports.

5. `make down`
```text
Container etl-project-3-app Removed
Container etl-project-3-localstack Removed
Network etl-project-3_etl-network Removed
```

## Local Workflow Components

1. `setup_local.py`
I use this to create local streams, DynamoDB table, and output bucket in LocalStack.

2. `kinesis_trip_producer.py`
I use this to publish `trip_start.csv` and `trip_end.csv` into their respective streams in batches.

3. `local_pipeline_runner.py`
I use this to pull stream records, invoke both Lambda handlers, and then run incremental aggregation in `local_staging` mode.

What happens during `make docker-runner`:
- `trip_start` writes state into DynamoDB.
- `trip_end` updates the same DynamoDB rows.
- `trip_end` also appends compact completed-trip JSONL rows into `staging/completed_trips/...` inside the LocalStack bucket.
- `glue_trip_aggregator.py` reads only the staged files that are newer than the checkpoint and rewrites only the affected hourly output partitions.

## One-command Run

```bash
make docker-all
```

## Notes from My Side

- This local flow simulates Lambda + Glue behavior, but it does not create real Lambda event source mappings.
- For AWS, I intentionally do manual Console setup described in `docs/AWS_DEPLOYMENT.md`.
- If I want host-mode execution instead of Docker, I can still run `make local-all` after installing dependencies locally.
