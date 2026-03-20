# AWS Deployment (Manual Console Setup)

AWS resources are set up manually via the Console — no IaC. This keeps the focus on pipeline logic rather than infrastructure tooling.

Before touching AWS, run a full local cycle first:
```bash
make up && make docker-all && make down
```
If local passes, AWS setup is straightforward.

---

## Naming I Use Everywhere

Keeping names consistent across services saves a lot of confusion later.

| Resource | Name |
|---|---|
| Kinesis stream (start) | `trip-start-stream` |
| Kinesis stream (end) | `trip-end-stream` |
| DynamoDB table | `trip_lifecycle` |
| Lambda (start) | `etl-project-3-trip-start` |
| Lambda (end) | `etl-project-3-trip-end` |
| Glue job | `etl-project-3-trip-aggregation` |
| Glue database | `etl_project_3_analytics` |
| Glue crawler | `etl-project-3-trip-metrics-crawler` |
| S3 bucket | `etl-project-3-analytics-<account-id>` |

---

## 1) S3 Bucket

1. Open `S3` → `Create bucket` in your target region.
2. Create these prefixes (just upload a placeholder file or create "folders" in the UI):
   - `scripts/`
   - `staging/completed_trips/`
   - `checkpoints/`
   - `aggregations/hourly_zone_metrics/`
   - `athena-results/`
3. Upload `scripts/glue_trip_aggregator.py` into `scripts/`.

The Glue job reads its script from S3, so this upload has to happen before step 7.

---

## 2) Kinesis Streams

1. `Kinesis` → `Data streams` → `Create data stream`.
2. Create two streams, both with **1 shard** (sufficient for the sample dataset):
   - `trip-start-stream`
   - `trip-end-stream`
3. Wait until both show `Active` status.

---

## 3) DynamoDB Table

1. `DynamoDB` → `Tables` → `Create table`.
2. Settings:
   - Table name: `trip_lifecycle`
   - Partition key: `trip_id` (String)
   - Billing mode: **On-demand**
3. No sort key, no secondary indexes needed.

---

## 4) IAM Roles

I create two roles — one for Lambda, one for Glue.

### Lambda role: `etl-project-3-lambda-role`

Trusted entity: Lambda service.

Attach managed policy: `AWSLambdaBasicExecutionRole` (gives CloudWatch Logs access).

Add an inline policy with these permissions:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["dynamodb:GetItem", "dynamodb:UpdateItem", "dynamodb:PutItem"],
      "Resource": "arn:aws:dynamodb:<region>:<account-id>:table/trip_lifecycle"
    },
    {
      "Effect": "Allow",
      "Action": [
        "kinesis:GetRecords", "kinesis:GetShardIterator",
        "kinesis:DescribeStream", "kinesis:DescribeStreamSummary",
        "kinesis:ListShards", "kinesis:ListStreams"
      ],
      "Resource": [
        "arn:aws:kinesis:<region>:<account-id>:stream/trip-start-stream",
        "arn:aws:kinesis:<region>:<account-id>:stream/trip-end-stream"
      ]
    },
    {
      "Effect": "Allow",
      "Action": "s3:PutObject",
      "Resource": [
        "arn:aws:s3:::<your-bucket>/staging/completed_trips/*",
        "arn:aws:s3:::<your-bucket>/checkpoints/*"
      ]
    }
  ]
}
```

Optional — only add this if you want Lambda to be able to force-start a Glue run:
```json
{
  "Effect": "Allow",
  "Action": "glue:StartJobRun",
  "Resource": "arn:aws:glue:<region>:<account-id>:job/etl-project-3-trip-aggregation"
}
```

### Glue role: `etl-project-3-glue-role`

Trusted entity: Glue service.

Attach managed policy: `AWSGlueServiceRole`.

Add inline S3 policy:
```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::<your-bucket>",
        "arn:aws:s3:::<your-bucket>/scripts/*",
        "arn:aws:s3:::<your-bucket>/staging/*",
        "arn:aws:s3:::<your-bucket>/checkpoints/*"
      ]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject"],
      "Resource": [
        "arn:aws:s3:::<your-bucket>/aggregations/*",
        "arn:aws:s3:::<your-bucket>/checkpoints/*"
      ]
    }
  ]
}
```

---

## 5) Lambda Functions (no zip, code editor only)

I paste code directly into the Lambda console editor. No packaging step.

### Function 1: `etl-project-3-trip-start`

1. `Lambda` → `Create function` → Author from scratch.
2. Runtime: `Python 3.12`, Architecture: `x86_64`.
3. Execution role: `etl-project-3-lambda-role`.
4. In the code editor: replace all content in `lambda_function.py` with everything from `scripts/lambda_trip_start.py`.
5. Handler: `lambda_function.handler`.
6. Click **Deploy**.
7. Environment variables:
   - `TRIP_LIFECYCLE_TABLE` = `trip_lifecycle`
   - `AWS_REGION` = `<your-region>`

Quick sanity check: go to `Test` tab → create a test event with body `{"Records":[]}` → Run. Should return `{"statusCode": 200, "body": "{\"processed\": 0, \"failed\": 0, ...}"}` with no import errors.

### Function 2: `etl-project-3-trip-end`

Same process as above, using `scripts/lambda_trip_end.py`.

Environment variables:
- `TRIP_LIFECYCLE_TABLE` = `trip_lifecycle`
- `AGGREGATION_BUCKET` = `<your-bucket>`
- `COMPLETED_TRIP_STAGING_PREFIX` = `staging/completed_trips`
- `AGGREGATION_CHECKPOINT_URI` = `s3://<your-bucket>/checkpoints/hourly_zone_metrics_checkpoint.json`
- `TRIGGER_GLUE` = `false`
- `AWS_REGION` = `<your-region>`

Optional — only set these if you want the top-routes second output:
- `TOP_ROUTES_OUTPUT_PATH` = `s3://<your-bucket>/aggregations/top_routes_hourly`
- `TOP_ROUTES_LIMIT` = `5`

---

## 6) Kinesis Triggers on Lambda

I use Kinesis event source mappings — this is AWS's native pull model. Lambda polls the stream automatically, no EventBridge rule needed.

For `etl-project-3-trip-start`:
- Trigger type: **Kinesis**
- Stream: `trip-start-stream`
- Starting position: `Latest`
- Batch size: `100`
- Batching window: `5 seconds`

For `etl-project-3-trip-end`:
- Trigger type: **Kinesis**
- Stream: `trip-end-stream`
- Starting position: `Latest`
- Batch size: `100`
- Batching window: `5 seconds`

Why I prefer this over EventBridge polling Lambda:
- AWS manages the Kinesis checkpoint (sequence number) for us
- Lambda only wakes up when records actually exist in the stream
- If the stream is quiet, Lambda is effectively idle — no wasted invocations

The 5-second batching window means Lambda waits up to 5 seconds to accumulate records before invoking — this reduces the number of small invocations during bursty traffic.

---

## 7) Glue Job

1. `Glue` → `ETL jobs` → `Script editor` → `Spark`.
2. Settings:
   - Name: `etl-project-3-trip-aggregation`
   - IAM role: `etl-project-3-glue-role`
   - Glue version: 4.0 (Spark 3.3, Python 3)
   - Script path: `s3://<your-bucket>/scripts/glue_trip_aggregator.py`
   - Temporary path: `s3://<your-bucket>/tmp/`
3. Job parameters (under `Advanced properties` → `Job parameters`):

| Key | Value |
|---|---|
| `--staging_bucket` | `<your-bucket>` |
| `--staging_prefix` | `staging/completed_trips` |
| `--checkpoint_uri` | `s3://<your-bucket>/checkpoints/hourly_zone_metrics_checkpoint.json` |
| `--output_path` | `s3://<your-bucket>/aggregations/hourly_zone_metrics` |
| `--region` | `<your-region>` |
| `--mode` | `glue_staging` |

Optional top-routes parameters:
| Key | Value |
|---|---|
| `--top_routes_output_path` | `s3://<your-bucket>/aggregations/top_routes_hourly` |
| `--top_routes_limit` | `5` |

### 7a) Glue Schedule

I don't want Lambda triggering Glue on every batch — that would queue up dozens of competing Glue runs. Instead, Glue runs on a fixed hourly schedule.

In the Glue job console: `Schedules` tab → `Create schedule`:
- Schedule expression: `cron(0 * * * ? *)` (every hour on the hour)
- Or for testing: `rate(15 minutes)`

**What this actually does under the hood:** the Glue console's Schedules tab is a convenience wrapper — when you save a schedule here, AWS automatically creates an EventBridge Scheduler rule that triggers this specific Glue job at the defined interval. You won't need to touch EventBridge directly; the rule is created and managed for you. This is what other parts of the project documentation refer to as the "EventBridge hourly schedule" — both terms describe the same mechanism.

The checkpoint ensures each run only processes staged files it hasn't seen yet — so running hourly vs. every 15 minutes doesn't cause double-counting, just changes how fresh the Athena output is.

If I ever need to force a run outside the schedule:
```bash
aws glue start-job-run \
  --job-name etl-project-3-trip-aggregation \
  --region <your-region>
```

---

## 8) Glue Database + Crawlers

1. `Glue` → `Databases` → `Add database`: `etl_project_3_analytics`.

2. `Glue` → `Crawlers` → `Create crawler`:
   - Name: `etl-project-3-trip-metrics-crawler`
   - Data source: `s3://<your-bucket>/aggregations/hourly_zone_metrics/`
   - IAM role: `etl-project-3-glue-role`
   - Target database: `etl_project_3_analytics`
   - Table name prefix: `trip_metrics_`
   - Schedule: on demand (or match the Glue job schedule)

Optional second crawler for top-routes:
- Name: `etl-project-3-top-routes-crawler`
- Data source: `s3://<your-bucket>/aggregations/top_routes_hourly/`
- Table name prefix: `trip_top_routes_`

---

## 9) Athena

1. Open `Athena` → `Settings` → set query result location to `s3://<your-bucket>/athena-results/`.
2. Select database `etl_project_3_analytics` in the query editor.
3. Run the crawler once to register table partitions, then use queries from `artifacts/sql/athena_queries.sql`.

---

## 10) End-to-end Verification

Publish events:
```bash
python3 scripts/kinesis_trip_producer.py --region "<your-region>"
```

Then check each layer in order:

1. **Lambda CloudWatch logs** — look for `staged_completed_trips > 0` in trip_end log group
2. **DynamoDB scan** — items should have both `start_pickup_location_id` and `end_dropoff_datetime`
3. **S3 staging** — files should appear under `staging/completed_trips/year=.../`
4. **Glue job history** — wait for scheduled run or trigger manually, check `SUCCEEDED`
5. **S3 output** — Parquet files under `aggregations/hourly_zone_metrics/year=.../`
6. **Athena** — run crawler, then `SELECT COUNT(*) FROM trip_metrics_hourly_zone_metrics`

---

## Cost Expectations

This pipeline is cheap at sample scale:

| Service | Notes |
|---|---|
| Kinesis | ~$0.015/hr per shard + PUT payload units |
| Lambda | Near zero — invocations are event-driven, not continuous |
| DynamoDB | On-demand — pay per read/write, no idle cost |
| Glue | ~$0.44/DPU-hour, 2 DPU minimum — 1-min run ≈ $0.01 |
| S3 | Storage + request costs, minimal at this scale |
| Athena | $5 per TB scanned — basically free for a few MBs of Parquet |
