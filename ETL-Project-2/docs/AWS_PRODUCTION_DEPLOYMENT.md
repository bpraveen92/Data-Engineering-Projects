# AWS Production Deployment Guide

Once the pipeline is validated locally, deploying to real AWS is a matter of wiring up the same script to real services — Kinesis instead of LocalStack, S3 instead of MinIO, and AWS Glue instead of a local `spark-submit`. The `spark_aggregator.py` script is already written to handle both environments; the only difference is that `--local` is not passed on Glue.

For a guide on running the pipeline locally first, see [`LOCAL_DEVELOPMENT_SETUP.md`](LOCAL_DEVELOPMENT_SETUP.md).

---

## Prerequisites

- AWS CLI configured (`aws configure`) with credentials for your target account
- An AWS account with access to S3, Kinesis, IAM, Glue, and CloudWatch

> **Note:** All AWS infrastructure in this project was created manually via the console to keep things simple and auditable. In a production setup, these would be created via Terraform or CloudFormation.

---

## Step 1: Create the S3 Bucket and Upload Assets

Create the S3 bucket via the AWS Console:

- **Bucket name:** `pravbala-data-engineering-projects`
- **Region:** `ap-south-2`

Inside the bucket, create the following folder structure and upload the files manually:

```
s3://pravbala-data-engineering-projects/
└── Project-2/
    ├── scripts/
    │   └── spark_aggregator.py           ← the main PySpark script
    ├── jars/
    │   └── spark-streaming-sql-kinesis-connector_2.12-1.0.0.jar  ← Kinesis connector
    ├── sample_data_initial_load/
    │   ├── songs.csv                     ← dimension table
    │   └── users.csv                     ← dimension table
    ├── aggregations/                     ← Glue writes Parquet output here
    └── checkpoints/                      ← Spark + connector state (auto-created)
```

The JAR and dimension CSVs are the same files used locally — no changes needed.

---

## Step 2: Create the Kinesis Stream

Create the Kinesis stream via the AWS Kinesis console:

- **Stream name:** `music-streams`
- **Region:** `ap-south-2`
- **Capacity mode:** Provisioned
- **Shard count:** 1

Once the stream status shows `Active`, it's ready to receive records.

---

## Step 3: Create the IAM Role for Glue

Create an IAM role via the IAM console that Glue uses to run the job:

- **Role name:** `etl-project-2`
- **Trusted entity:** AWS Glue service (`glue.amazonaws.com`)

Attach the following policies:

| Policy | Why |
|--------|-----|
| `AWSGlueServiceRole` | Glue job execution basics |
| `AmazonKinesisReadOnlyAccess` | Read from `music-streams` |
| `AmazonS3FullAccess` | Read dimension tables and write Parquet output + checkpoints |
| `CloudWatchLogsFullAccess` | Write logs visible in CloudWatch |

> For a real production system, scope these down to specific resource ARNs rather than using broad managed policies.

---

## Step 4: Create the Glue Job

Create the Glue streaming job via the AWS Glue console (Glue → ETL Jobs → Create job → Spark script editor):

**Job settings:**

| Setting | Value |
|---------|-------|
| Name | `etl-project-2-streaming` |
| Type | Spark Streaming |
| Glue version | Glue 4.0 (Spark 3.3, Python 3) |
| IAM Role | `etl-project-2` |
| Script location | `s3://pravbala-data-engineering-projects/Project-2/scripts/spark_aggregator.py` |
| Temporary directory | `s3://aws-glue-assets-<account-id>-ap-south-2/temporary/` |
| Worker type | G.1X |
| Number of workers | 2 |
| Max concurrent runs | 1 |

**Extra JARs** (under Job details → Advanced properties → Dependent JARs path):

```
s3://pravbala-data-engineering-projects/Project-2/jars/spark-streaming-sql-kinesis-connector_2.12-1.0.0.jar
```

**Job parameters** (under Job details → Advanced properties → Job parameters):

| Key | Value |
|-----|-------|
| `--kinesis-stream` | `music-streams` |
| `--songs-path` | `s3://pravbala-data-engineering-projects/Project-2/sample_data_initial_load/songs.csv` |
| `--users-path` | `s3://pravbala-data-engineering-projects/Project-2/sample_data_initial_load/users.csv` |
| `--output-path` | `s3://pravbala-data-engineering-projects/Project-2/aggregations` |
| `--checkpoint-path` | `s3://pravbala-data-engineering-projects/Project-2/checkpoints` |
| `--region` | `ap-south-2` |
| `--window-minutes` | `2` |
| `--watermark-minutes` | `0.5` |

> **Note on `--songs-path` and `--users-path`:** These use the `s3://` scheme (not `s3a://`) — Glue's native S3 filesystem handles `s3://` reads fine for static CSVs. The `s3a://` scheme is only required for paths that go through Hadoop's `FileContext` (i.e., the Kinesis connector's `metadataPath` and the Spark `checkpointLocation`).

---

## Step 5: Run the Producer from Your Local Machine

The producer runs on the Mac host directly — not inside Docker. It uses real AWS credentials and targets the real Kinesis stream:

```bash
make aws-producer

# Which runs:
python3 scripts/kinesis_stream_producer.py \
  --stream-name music-streams \
  --region ap-south-2 \
  --batch-size 20 \
  --interval-seconds 5.0
  # Note: no --local flag — targets real AWS
```

Make sure your AWS credentials are configured:

```bash
aws configure  # or export AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY
```

Expected output:

```
INFO - Stream 'music-streams' already exists
INFO - Starting producer: music-streams
INFO - [Batch 1] Sent 20 events (total: 20)
INFO - [Batch 2] Sent 20 events (total: 40)
...
```

---

## Step 6: Start the Glue Job

```bash
make glue-start

# Which runs:
aws glue start-job-run \
  --job-name etl-project-2-streaming \
  --region ap-south-2
```

Or trigger it from the Glue console. The job takes approximately 90 seconds to provision a worker before any Python code runs — that's normal for Glue.

Monitor status:

```bash
make glue-status

# Which runs:
aws glue get-job-runs \
  --job-name etl-project-2-streaming \
  --region ap-south-2 \
  --query 'JobRuns[0].{State:JobRunState,Started:StartedOn,Duration:ExecutionTime,Error:ErrorMessage}'
```

---

## Step 7: Verify Output in S3

Once the job has been running for a few minutes (long enough for the first window to close and flush), check for Parquet files:

```bash
aws s3 ls s3://pravbala-data-engineering-projects/Project-2/aggregations/ \
  --recursive \
  --region ap-south-2
```

Expected output:

```
2026-03-04 ... hourly_streams/window_start=2026-03-04 10:55:00/part-00000-....snappy.parquet
2026-03-04 ... top_tracks_hourly/window_start=2026-03-04 10:55:00/part-00000-....snappy.parquet
2026-03-04 ... country_metrics_hourly/window_start=2026-03-04 10:55:00/part-00000-....snappy.parquet
```

---

## Step 8: Stop the Glue Job When Done

Glue streaming jobs run indefinitely and accumulate DPU-hours. Stop the job when you're finished:

```bash
# Get the current run ID
aws glue get-job-runs \
  --job-name etl-project-2-streaming \
  --region ap-south-2 \
  --query 'JobRuns[0].Id' \
  --output text

# Stop it
aws glue batch-stop-job-run \
  --job-name etl-project-2-streaming \
  --job-run-ids <run-id> \
  --region ap-south-2
```

---

## Deployment Summary

| Step | What | How |
|------|------|-----|
| 1 | Create S3 bucket, upload JAR + CSVs + script | AWS Console (manual) |
| 2 | Create Kinesis stream `music-streams` (1 shard, Provisioned) | AWS Console (manual) |
| 3 | Create IAM role `etl-project-2` with Glue + Kinesis + S3 + CloudWatch policies | IAM Console (manual) |
| 4 | Create Glue streaming job pointing at the S3 script + JAR | Glue Console (manual) |
| 5 | Run producer from Mac against real Kinesis | `make aws-producer` |
| 6 | Start Glue job | `make glue-start` |
| 7 | Verify Parquet output in S3 | `aws s3 ls ...` |
| 8 | Stop Glue job | `aws glue batch-stop-job-run ...` |

---

## Troubleshooting

For every error encountered during deployment (argparse crashes, Kinesis connector config quirks, checkpoint state mismatches, etc.) see [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md).
