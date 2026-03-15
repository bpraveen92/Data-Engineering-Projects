# Execution Guide

This is the runbook I use to run the project locally and on AWS.

## A) Local Simulation

Approx runtime I usually see on my machine:

| Command | Typical Time |
|---|---|
| `make up` | 30 sec to 90 sec (first build can take 5 to 8 min) |
| `make docker-setup-local` | 20 sec to 40 sec |
| `make docker-producer` | 60 sec to 120 sec |
| `make docker-runner` | 6 min to 10 min |
| `make down` | 10 sec to 30 sec |

### 1) Start Docker Stack

```bash
make up
```

Sample output from one of my runs:

```text
Container etl-project-3-localstack Created
Container etl-project-3-localstack Started
Container etl-project-3-localstack Healthy
Container etl-project-3-app Started
```

### 2) Create Local Resources

```bash
make docker-setup-local
```

Sample output:

```text
INFO Creating Kinesis stream: trip-start-stream
INFO Creating Kinesis stream: trip-end-stream
INFO Creating DynamoDB table: trip_lifecycle
INFO Creating S3 bucket: etl-project-3-analytics-local
WARNING Skipping Glue database check: glue service is not included in LocalStack community license
INFO Local setup complete.
INFO Streams: trip-start-stream, trip-end-stream
INFO Table: trip_lifecycle
INFO Output bucket: etl-project-3-analytics-local
```

At this point I expect:
- Kinesis streams: `trip-start-stream`, `trip-end-stream`
- DynamoDB table: `trip_lifecycle`
- Local analytics bucket: `etl-project-3-analytics-local`

### 3) Publish Input Streams

```bash
make docker-producer
```

Sample output:

```text
INFO Starting publish with start_rows=4999 end_rows=4999 batch_size=100 endpoint=http://etl-project-3-localstack:4566
INFO trip_start -> trip-start-stream sent=100 total=100
INFO trip_end -> trip-end-stream sent=100 total=100
...
INFO trip_start -> trip-start-stream sent=99 total=4999
INFO trip_end -> trip-end-stream sent=99 total=4999
INFO Publishing complete. start=4999 end=4999
```

What should happen:
- `trip_start.csv` goes to `trip-start-stream`
- `trip_end.csv` goes to `trip-end-stream`

### 4) Process Streams + Aggregate

```bash
make docker-runner
```

Sample output:

```text
INFO Fetched stream records: trip_start=4999 trip_end=4999
INFO trip_start batch result={'processed': 100, 'failed': 0, 'table': 'trip_lifecycle'}
...
WARNING Skipping invalid trip_end event; missing=['rate_code', 'passenger_count', 'payment_type', 'trip_type'] payload={...}
INFO trip_end batch result={'processed': 92, 'failed': 8, 'upserted_without_start': 0, 'staged_completed_trips': 92, 'table': 'trip_lifecycle', 'glue_job_name': None, 'glue_run_id': None}
...
INFO trip_start summary={'processed': 4999, 'failed': 0, 'staged_completed_trips': 0}
INFO trip_end summary={'processed': 4468, 'failed': 531, 'staged_completed_trips': 4468}
INFO Local incremental aggregation completed summary={'metric_rows': 3460, 'affected_hours': 25, 'top_route_rows': 0, 'new_objects': 1145}
INFO Local pipeline completed. Aggregation output path=/workspace/output/aggregations
```

This flow does four things in order:
1. Reads both Kinesis streams.
2. Invokes `lambda_trip_start.handler`.
3. Invokes `lambda_trip_end.handler`.
4. Appends valid completed trips into staged S3 files, then runs incremental aggregation and writes parquet to `./output/aggregations`.

Optional analytical view (top routes per hour):

```bash
export TOP_ROUTES_OUTPUT_PATH=./output/top_routes_hourly
export TOP_ROUTES_LIMIT=5
make docker-runner
```

That adds a second output dataset with route ranking metrics per hour:
- `route_rank_in_hour`
- `avg_fare_per_trip`
- `avg_tip_per_trip`
- `tip_to_fare_ratio`

Sample validation output I got:

```text
rows 125
columns ['event_hour', 'pickup_location_id', 'dropoff_location_id', 'trip_count', 'fare_sum', 'tip_sum', 'distance_sum', 'route_rank_in_hour', 'avg_fare_per_trip', 'avg_tip_per_trip', 'tip_to_fare_ratio', 'year', 'month', 'day', 'hour']
sample [{'event_hour': datetime.datetime(2024, 5, 25, 0, 0), 'pickup_location_id': 82, 'dropoff_location_id': 138, 'trip_count': 3, 'route_rank_in_hour': 1, 'avg_fare_per_trip': 69.273536, 'tip_to_fare_ratio': 0.02059, ...}]
```

### 5) Validate Output

```bash
find output/aggregations -type f | sort
```

I also validate the final state with these commands:

```bash
docker exec etl-project-3-app python3 -c "import boto3; c=boto3.client('dynamodb',region_name='ap-south-2',endpoint_url='http://etl-project-3-localstack:4566',aws_access_key_id='test',aws_secret_access_key='test'); table='trip_lifecycle'; total=completed=missing_end=0; last=None
while True:
 resp=c.scan(TableName=table,ExclusiveStartKey=last) if last else c.scan(TableName=table)
 items=resp.get('Items',[])
 total+=len(items)
 for it in items:
  if it.get('trip_status',{}).get('S','')=='completed': completed+=1
  if 'end_dropoff_datetime' not in it: missing_end+=1
 last=resp.get('LastEvaluatedKey')
 if not last: break
print({'total_items':total,'completed_items':completed,'missing_end_items':missing_end})"
```

```text
{'total_items': 4999, 'completed_items': 4468, 'missing_end_items': 531}
```

```bash
docker exec etl-project-3-app python3 -c "from pathlib import Path; base=Path('/workspace/output/aggregations'); hours=sorted([p for p in base.rglob('hour=*') if p.is_dir()]); files=list(base.rglob('*.parquet')); print({'hour_partitions':len(hours),'parquet_files':len(files),'first_partition':str(hours[0]) if hours else None,'last_partition':str(hours[-1]) if hours else None})"
```

```text
{'hour_partitions': 25, 'parquet_files': 25, 'first_partition': '/workspace/output/aggregations/year=2024/month=05/day=25/hour=00', 'last_partition': '/workspace/output/aggregations/year=2024/month=05/day=26/hour=00'}
```

I expect partition paths like:
- `year=2024/month=05/day=25/hour=00`
- ...
- `year=2024/month=05/day=26/hour=00` (late dropoff spillover)

### 6) Stop Local Stack

```bash
make down
```

Sample output:

```text
Container etl-project-3-app Removed
Container etl-project-3-localstack Removed
Network etl-project-3_etl-network Removed
```

## Local Code Walkthrough

When I debug locally, I trace execution like this:

1. `local_pipeline_runner.main()`
- Pulls stream records via `fetch_stream_records`.
- Invokes Lambda handlers through `run_in_batches`.

2. `lambda_trip_start.handler()`
- Decodes each Kinesis record.
- Validates required fields.
- Writes trip start state using `write_trip_start`.

3. `lambda_trip_end.handler()`
- Decodes each Kinesis record.
- Validates required fields.
- Updates or upserts trip lifecycle state using `write_trip_end`.
- Builds compact completed-trip rows and appends them to `staging/completed_trips/...`.

4. `run_local_staging_aggregation()` in `glue_trip_aggregator.py`
- Reads only the newly affected staged JSONL files from LocalStack S3.
- Uses the checkpoint object to skip staged files that were already processed in earlier runs.
- Applies `transform_completed_trips`.
- Writes partitioned parquet output.

Why this local read path is different from AWS:
- Local mode uses boto3 against LocalStack S3 so Docker runs stay simple.
- AWS Glue mode reads the affected staged S3 prefixes directly with Spark inside Glue runtime.
- I keep both so local tests stay practical while AWS runs stay close to the real scheduled Glue behavior.

## B) AWS Execution

### 1) Configure Environment

```bash
cp .env.example .env
# set your real account values (region, bucket, names)
```

### 2) Manually Create AWS Prerequisites

Before sending data, I manually create infra in AWS Console. I follow `docs/AWS_DEPLOYMENT.md` in this order:
- S3 bucket and Glue script upload
- Kinesis streams
- DynamoDB table
- IAM roles and policies
- Lambda functions and stream triggers
- Glue job
- Glue database + crawler
- Athena output location

Note:
- Lambda creation is UI-only in this project. I do not create Lambda resources via code or CLI.

### 3) Send Data to Kinesis

```bash
python3 scripts/kinesis_trip_producer.py --endpoint-url "" --region "$AWS_REGION"
```

### 4) Verify Stateful Trip Table

```bash
aws dynamodb scan \
  --table-name "$TRIP_LIFECYCLE_TABLE" \
  --region "$AWS_REGION" \
  --max-items 5
```

I usually check for:
- both `start_*` and `end_*` fields
- `trip_status=completed` for ended trips
- `record_source` showing update vs upsert path

### 5) Verify Glue Triggering

```bash
aws glue get-job-runs \
  --job-name "$GLUE_JOB_NAME" \
  --region "$AWS_REGION" \
  --max-results 5
```

### 6) Refresh Catalog + Query in Athena

```bash
aws glue start-crawler --name etl-project-3-trip-metrics-crawler --region "$AWS_REGION"
```

If top-routes output is enabled in Lambda/Glue job args:

```bash
aws glue start-crawler --name etl-project-3-top-routes-crawler --region "$AWS_REGION"
```

Then I run SQL from `artifacts/sql/athena_queries.sql`.

## Event Contract Examples

### `trip_start`

```json
{
  "trip_id": "c66ce556bc",
  "pickup_location_id": "93",
  "dropoff_location_id": "93",
  "vendor_id": "1",
  "pickup_datetime": "2024-05-25 13:19:00",
  "estimated_dropoff_datetime": "2024-05-25 14:03:00",
  "estimated_fare_amount": "34.18595283806629"
}
```

### `trip_end`

```json
{
  "trip_id": "c66ce556bc",
  "dropoff_datetime": "2024-05-25 14:05:00",
  "rate_code": "5.0",
  "passenger_count": "0.0",
  "trip_distance": "0.1",
  "fare_amount": "40.09620692463166",
  "tip_amount": "0.0",
  "payment_type": "3.0",
  "trip_type": "2.0"
}
```

### Example hourly aggregate row

```json
{
  "event_hour": "2024-05-25 14:00:00",
  "pickup_location_id": 93,
  "dropoff_location_id": 93,
  "trip_count": 17,
  "fare_sum": 480.129,
  "tip_sum": 72.65,
  "distance_sum": 31.4,
  "year": "2024",
  "month": "05",
  "day": "25",
  "hour": "14"
}
```
