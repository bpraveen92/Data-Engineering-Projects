# Deploying to AWS EMR Serverless

> **Alternative deployment — currently blocked.** The awslabs Kinesis connector does not support `Trigger.AvailableNow` (GitHub Issue #79, open March 2026). This breaks the scheduled drain-and-exit architecture that makes EMR Serverless cost-effective.
>
> **Primary deployment is AWS Glue Streaming** — see [`GLUE_DEPLOYMENT.md`](GLUE_DEPLOYMENT.md).
>
> **When this becomes viable:** If Issue #79 is resolved, EMR Serverless saves ~$600/month (~$0.50–1/day vs Glue's ~$21/day). No code changes needed — just switch `--trigger-mode` to `available_now` and delete the stale S3 checkpoint.

---

## Why EMR is Blocked — Root Cause

`Trigger.AvailableNow` requires `SupportsAdmissionControl.reportLatestOffset()` to give Spark a read fence. The awslabs connector implements `latestOffset()` but **not** `reportLatestOffset()`. Without the fence, Spark exits immediately — no partition readers run.

Because partition readers never run, the connector's `shard-source/` checkpoint layer is never written. On the next job, the fallback reads `TRIM_HORIZON` from the Spark `offsets/` log, replays from the beginning of the shard, and all records are dropped as late by the watermark advanced in prior runs. The job exits `SUCCESS` with zero output.

**Confirmed evidence (job `00g3v9llj3al201v`, 7 March 2026):**
```json
// offsets/1 — iteratorPosition is empty, not a real sequence number
{"shardId-000000000000": {"iteratorType": "TRIM_HORIZON", "iteratorPosition": ""}}
```
Watermark from last successful run: `2026-03-07T12:09 UTC` — any earlier record is silently dropped.

**What to verify if Issue #79 is fixed:**
1. `shard-source/` exists after each run and contains `AfterSequenceNumber` entries.
2. Job N+1 processes only records newer than job N's last sequence number.
3. Delete stale S3 checkpoint before switching back.

---

## Setup Steps

### Step 1 — Create S3 bucket and upload assets

Bucket: `pravbala-data-engineering-projects` in `ap-south-2`

```
s3://pravbala-data-engineering-projects/
└── Project-2/
    ├── scripts/spark_aggregator.py
    ├── jars/hadoop-aws-3.3.4.jar
    ├── jars/aws-java-sdk-bundle-1.12.565.jar
    ├── sample_data_initial_load/songs.csv
    ├── sample_data_initial_load/users.csv
    ├── aggregations/           ← Spark creates on first write
    └── checkpoints/            ← Spark creates on first write
```

```bash
aws s3 cp scripts/spark_aggregator.py \
  s3://pravbala-data-engineering-projects/Project-2/scripts/ --region ap-south-2

aws s3 cp sample_data_initial_load/songs.csv \
  s3://pravbala-data-engineering-projects/Project-2/sample_data_initial_load/ --region ap-south-2

aws s3 cp sample_data_initial_load/users.csv \
  s3://pravbala-data-engineering-projects/Project-2/sample_data_initial_load/ --region ap-south-2

aws s3 cp jars/hadoop-aws-3.3.4.jar \
  s3://pravbala-data-engineering-projects/Project-2/jars/ --region ap-south-2

aws s3 cp jars/aws-java-sdk-bundle-1.12.565.jar \
  s3://pravbala-data-engineering-projects/Project-2/jars/ --region ap-south-2
```

> The Kinesis connector is bundled in the EMR 7.1.0+ runtime image at `/usr/share/aws/kinesis/spark-sql-kinesis/lib/` — no S3 upload needed.

### Step 2 — Create Kinesis stream

- **Name:** `music-streams` · **Region:** `ap-south-2` · **Mode:** Provisioned · **Shards:** 1

### Step 3 — Create IAM execution role

- **Name:** `etl-project-2-emr-execution` · **Trusted entity:** `emr-serverless.amazonaws.com`
- Policies: `AmazonKinesisReadOnlyAccess`, `AmazonS3FullAccess`, `CloudWatchLogsFullAccess`

### Step 4 — Create EMR Serverless application

| Setting | Value |
|---------|-------|
| Name | `etl-project-2` |
| Release | `emr-7.12.0` (Spark 3.5, includes Kinesis connector) |
| Type | Spark |
| Pre-initialized capacity | Off |

Note the **Application ID** — needed for `make emr-start`.

### Step 5 — Run the producer

```bash
export AWS_ACCESS_KEY_ID=<your-key>
export AWS_SECRET_ACCESS_KEY=<your-secret>

make aws-producer
```

### Step 6 — Submit the job

```bash
export EMR_APP_ID=<your-application-id>
export EMR_EXECUTION_ROLE_ARN=arn:aws:iam::<account-id>:role/etl-project-2-emr-execution

make emr-start
```

Check status:
```bash
make emr-status EMR_APP_ID=<your-app-id>
```

### Step 7 — Set up EventBridge schedule

**EventBridge → Schedules → Create schedule:**

| Setting | Value |
|---------|-------|
| Name | `etl-project-2-emr-30min` |
| Rate | `rate(30 minutes)` |
| Target | EMR Serverless → `StartJobRun` |
| Application ID | from Step 4 |

Add `emr-serverless:StartJobRun` as an inline policy on `etl-project-2-emr-execution`.

### Step 8 — Verify output

```bash
aws s3 ls s3://pravbala-data-engineering-projects/Project-2/aggregations/ \
  --recursive --region ap-south-2
```

---

## Deployment Summary

| Step | What | How |
|------|------|-----|
| 1 | S3 bucket + assets | Console + `aws s3 cp` |
| 2 | Kinesis stream | Kinesis Console |
| 3 | IAM execution role | IAM Console |
| 4 | EMR Serverless application | EMR Console |
| 5 | Run producer | `make aws-producer` |
| 6 | Submit job | `make emr-start` |
| 7 | EventBridge schedule | EventBridge Console |
| 8 | Verify S3 output | `aws s3 ls ...` |

---

## Troubleshooting

See [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md) — specifically:
- **G3** — `ClassNotFoundException: aws-kinesis.DefaultSource`: connector JAR must be passed via `--jars` with the local EMR image path (already set in `make emr-start`).
- **G5** — Job SUCCESS but zero output: the `availableNow` bug — root cause documented above and in TROUBLESHOOTING.md.
