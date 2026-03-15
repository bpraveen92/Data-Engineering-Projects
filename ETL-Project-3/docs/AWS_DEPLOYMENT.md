# AWS Deployment Guide (Manual Console Setup)

For this project, I’m intentionally doing AWS setup manually in the Console so the workflow stays realistic and not over-automated.
I do not create Lambda functions through CLI scripts or IaC in this project.

Before I start AWS setup, I always run one full local Docker cycle (`make up` + `make docker-all`) to make sure the logic is stable.

## Prerequisites

Before I start, I make sure I have:
- AWS account access with IAM, Kinesis, Lambda, DynamoDB, Glue, S3, Athena, and CloudWatch Logs permissions.
- A target AWS region (examples below use `ap-south-2`).
- Python 3.10+ locally.

## Naming I Keep Consistent

I use these names across services to avoid confusion:
- Kinesis: `trip-start-stream`, `trip-end-stream`
- DynamoDB: `trip_lifecycle`
- Lambda: `etl-project-3-trip-start`, `etl-project-3-trip-end`
- Glue job: `etl-project-3-trip-aggregation`
- Glue crawler: `etl-project-3-trip-metrics-crawler`
- Glue database: `etl_project_3_analytics`
- S3 bucket: unique bucket like `etl-project-3-analytics-<account-id>`

## 1) S3 Setup (Console)

1. Open `S3` in Console.
2. Create a bucket in your target region.
3. Create folders:
- `scripts/`
- `staging/completed_trips/`
- `checkpoints/`
- `aggregations/hourly_zone_metrics/`
- `athena-results/`
4. Upload `scripts/glue_trip_aggregator.py` into `scripts/`.

## 2) Kinesis Streams (Console)

1. Open `Kinesis` -> `Data streams`.
2. Create:
- `trip-start-stream` (1 shard)
- `trip-end-stream` (1 shard)
3. Wait until both are `Active`.

## 3) DynamoDB Table (Console)

1. Open `DynamoDB` -> `Tables` -> `Create table`.
2. Table name: `trip_lifecycle`.
3. Partition key: `trip_id` (String).
4. Billing mode: `On-demand`.

## 4) IAM Roles (Console)

I create two roles.

### Lambda role: `etl-project-3-lambda-role`

- Trusted entity: Lambda
- Managed policy: `AWSLambdaBasicExecutionRole`
- Inline permissions:
- `dynamodb:GetItem`, `dynamodb:UpdateItem`, `dynamodb:PutItem` on `trip_lifecycle`
- `kinesis:GetRecords`, `kinesis:GetShardIterator`, `kinesis:DescribeStream`, `kinesis:DescribeStreamSummary`, `kinesis:ListShards` on both streams
- `s3:PutObject` on `s3://<your-bucket>/staging/completed_trips/*`
- `s3:PutObject` on `s3://<your-bucket>/checkpoints/*`
- Optional only if I want Lambda to force-start Glue: `glue:StartJobRun` on Glue job

### Glue role: `etl-project-3-glue-role`

- Trusted entity: Glue
- Managed policy: `AWSGlueServiceRole`
- S3 access to:
- `s3://<your-bucket>/scripts/*`
- `s3://<your-bucket>/aggregations/*`

## 5) Create Lambda Functions in Console UI (No Zip Packaging)

I keep this project simple for portfolio purposes.
I do not package Lambda artifacts or run deploy scripts.
I create functions in Console and paste code directly from local `.py` files.

### Function 1: `etl-project-3-trip-start`
- Runtime: Python 3.12
- Role: `etl-project-3-lambda-role`
- In code editor: copy everything from `scripts/lambda_trip_start.py` into `lambda_function.py`
- Handler: `lambda_function.handler`
- Env vars:
- `TRIP_LIFECYCLE_TABLE=trip_lifecycle`
- `AWS_REGION=<your-region>`
- Click `Deploy` in Lambda code editor.

### Function 2: `etl-project-3-trip-end`
- Runtime: Python 3.12
- Role: `etl-project-3-lambda-role`
- In code editor: copy everything from `scripts/lambda_trip_end.py` into `lambda_function.py`
- Handler: `lambda_function.handler`
- Env vars:
- `TRIP_LIFECYCLE_TABLE=trip_lifecycle`
- `AGGREGATION_BUCKET=<your-bucket>`
- `COMPLETED_TRIP_STAGING_PREFIX=staging/completed_trips`
- `AGGREGATION_CHECKPOINT_URI=s3://<your-bucket>/checkpoints/hourly_zone_metrics_checkpoint.json`
- `TRIGGER_GLUE=false`
- `AWS_REGION=<your-region>`
- Optional for top-routes dataset:
- `TOP_ROUTES_OUTPUT_PATH=s3://<your-bucket>/aggregations/top_routes_hourly`
- `TOP_ROUTES_LIMIT=5`
- Click `Deploy` in Lambda code editor.

Quick sanity check I do after each function:
- Open `Test` tab and create a temporary event with `{"Records":[]}`.
- Run test once and confirm code loads without import/handler errors.

## 6) Kinesis Triggers on Lambda

For `etl-project-3-trip-start`:
- Trigger type: Kinesis
- Stream: `trip-start-stream`
- Starting position: `Latest`
- Batch size: `100`
- Maximum batching window: `5 seconds`

For `etl-project-3-trip-end`:
- Trigger type: Kinesis
- Stream: `trip-end-stream`
- Starting position: `Latest`
- Batch size: `100`
- Maximum batching window: `5 seconds`

Why I use this instead of EventBridge:
- Kinesis event source mapping is already the native pull model for Lambda.
- AWS keeps polling the stream on our behalf, tracks checkpoints, and only invokes Lambda when records are available.
- That means I do not need a scheduled EventBridge rule just to wake Lambda up and ask Kinesis if anything is there.

What this looks like day to day:
- If no new records arrive, Lambda stays effectively idle.
- If a few records arrive, Lambda is invoked in small batches after the batching window or batch size is met.
- If traffic grows, throughput is still bounded by shard count, so shard sizing matters.

Operational limits I keep in mind:
- Concurrency is tied closely to Kinesis shard parallelism, so under-sharded streams can create lag.
- If one record in a batch keeps failing and batch failure handling is not tuned well, retries can hold the shard back.
- Glue startup latency is much higher than Lambda latency, so I do not want every `trip_end` batch to start a Glue run.
- The cleaner shape here is to let Lambda append compact completed-trip events to S3, then let a scheduled Glue job reprocess only the affected staged hours.

What I would monitor in production:
- Lambda `IteratorAge` for both stream consumers
- Lambda error count and retry behavior
- Kinesis read throughput and shard pressure
- Glue concurrent job runs and queueing
- DynamoDB throttling or hot partition behavior on `trip_id`

## 7) Glue Job

1. Open Glue -> `ETL jobs` -> `Create job` (Spark).
2. Name: `etl-project-3-trip-aggregation`.
3. Role: `etl-project-3-glue-role`.
4. Script path: `s3://<your-bucket>/scripts/glue_trip_aggregator.py`.
5. Glue version: 4.0 (or current supported Spark runtime).
6. Job parameters:
- `--staging_bucket=<your-bucket>`
- `--staging_prefix=staging/completed_trips`
- `--checkpoint_uri=s3://<your-bucket>/checkpoints/hourly_zone_metrics_checkpoint.json`
- `--output_path=s3://<your-bucket>/aggregations/hourly_zone_metrics`
- `--region=<your-region>`
- Optional for second analytical output:
- `--top_routes_output_path=s3://<your-bucket>/aggregations/top_routes_hourly`
- `--top_routes_limit=5`

Why this differs from local:
- Local Docker testing uses boto3 against LocalStack S3 for staged-file reads (`local_staging` mode).
- AWS job reads the affected staged S3 prefixes directly inside Glue runtime (`glue_staging` mode).

## 7.1) Glue Scheduling

This is the piece I changed after the first cut of the project.
I no longer want `trip_end` Lambda to kick off a full Glue run every time it sees a successful batch.

What I do instead:
- keep `TRIGGER_GLUE=false` on `etl-project-3-trip-end`
- create an EventBridge schedule for the Glue job, usually every hour
- let Glue read only the staged files that arrived after the last checkpoint
- let Glue overwrite only the affected hourly partitions in the curated output

If I ever want to force an on-demand run for a demo, I can still temporarily set:
- `GLUE_JOB_NAME=etl-project-3-trip-aggregation`
- `TRIGGER_GLUE=true`

But that is not the daily operating mode I recommend.

## 8) Glue Database + Crawler

1. Glue -> `Databases` -> create `etl_project_3_analytics`.
2. Glue -> `Crawlers` -> create crawler:
- Name: `etl-project-3-trip-metrics-crawler`
- Source: `s3://<your-bucket>/aggregations/hourly_zone_metrics/`
- Target DB: `etl_project_3_analytics`
- Table prefix: `trip_metrics_`

Optional second crawler for top-routes output:
- Name: `etl-project-3-top-routes-crawler`
- Source: `s3://<your-bucket>/aggregations/top_routes_hourly/`
- Target DB: `etl_project_3_analytics`
- Table prefix: `trip_top_routes_`

## 9) Athena Setup

1. Open Athena.
2. Set query result location to `s3://<your-bucket>/athena-results/`.
3. Confirm `etl_project_3_analytics` is available.

## 10) Send Data + Verify

Publish the sample events:

```bash
python3 scripts/kinesis_trip_producer.py --endpoint-url "" --region "<your-region>"
```

Then I verify:
1. Lambda CloudWatch logs show successful batch processing.
2. DynamoDB table has completed records.
3. `trip_end` Lambda is writing compact staged JSONL files into `staging/completed_trips/`.
4. Scheduled Glue runs are picking up only new staged files after the checkpoint.
5. Curated output lands under `aggregations/hourly_zone_metrics/`.
6. Crawler refresh works and Athena queries return rows.
7. If top-routes is enabled, second top-routes crawler also returns rows in Athena.

## AWS Runtime Flow in Code

When the AWS resources are live, the runtime sequence is:

1. `kinesis_trip_producer.py` publishes start/end events to two streams.
2. `lambda_trip_start.handler` writes start-side fields to `trip_lifecycle`.
3. `lambda_trip_end.handler` updates end-side fields and marks trip completion.
4. `lambda_trip_end.handler` appends valid completed-trip rows into `staging/completed_trips/`.
5. Scheduled Glue runs call `run_glue_staging_aggregation` and rewrite only the affected hourly parquet partitions (+ top-routes parquet when enabled).
6. Glue crawler updates catalog so Athena queries can read fresh partitions.

## Cost Notes

The main cost drivers I monitor are:
- Kinesis shard-hour + PUT payload units
- Lambda invocation/duration
- DynamoDB on-demand usage
- Glue runtime
- S3 storage + Athena scanned bytes
