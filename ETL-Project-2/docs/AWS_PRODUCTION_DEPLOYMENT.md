# Deploying to AWS EMR Serverless

> **Caveat:** I've personally tested this pipeline on both EMR Serverless and AWS Glue Streaming. Both work with the same `spark_aggregator.py` script — no code changes needed between the two. If you want the Glue setup instead, head over to [`GLUE_DEPLOYMENT.md`](GLUE_DEPLOYMENT.md).

---

## Why I went with EMR Serverless

Let me be upfront about the trade-off here. This project works with data in the thousands — synthetic music streaming events I'm generating with a producer script, not millions of real concurrent listeners. The analytics we're computing (hourly stream counts, top tracks, country metrics) have a comfortable 1-hour latency SLA. Nobody's going to notice if the dashboard is 30 minutes behind.

With that context, running a Glue Streaming job 24/7 made no sense to me. At ~$21/day for a G.1X worker sitting idle between micro-batches, that's over $600/month to process a few thousand events an hour. EMR Serverless on a 30-minute EventBridge schedule costs me roughly $0.50–1/day. The compute wakes up, drains whatever's accumulated in Kinesis since the last run, writes Parquet to S3, saves the checkpoint, and dies. Clean, cheap, and fits the SLA easily.

That said — if I were running this at a real music platform with millions of concurrent listeners where a 5-minute lag means stale recommendation feeds and unhappy product managers, I'd flip to Glue Streaming (or Kinesis Data Analytics) immediately. At that scale, the $600/month becomes a rounding error compared to the business value. I've documented exactly how to do that in [`GLUE_DEPLOYMENT.md`](GLUE_DEPLOYMENT.md).

The one-line difference between the two deployments is this argument:
- EMR Serverless → `--trigger-mode available_now` (drain Kinesis backlog then exit)
- Glue Streaming → `--trigger-mode continuous` (run forever, poll every few seconds)

---

## How the scheduled execution works

It's worth spending a minute on this before jumping into the steps, because the checkpoint is what makes the whole thing work reliably across runs.

```
EventBridge Scheduler fires every 30 min
        ↓
EMR Serverless job starts (~30–60s cold start)
        ↓
Spark reads S3 checkpoint → knows the last committed Kinesis shard offset
        ↓
Reads only the records that arrived since the last run
        ↓
Runs windowed aggregations → writes Parquet partitions to S3
        ↓
Saves updated checkpoint back to S3
        ↓
Job exits cleanly → compute scales to zero, billing stops
        ↓
30 min later → repeat
```

The S3 checkpoint is the thread connecting each run to the next. It stores both the Spark streaming state (which micro-batches have been committed, watermark position, aggregation state) and the Kinesis connector's shard offsets. As long as runs happen at least once every 24 hours — Kinesis's default retention window — you'll never lose a record or reprocess one.

---

## Prerequisites

- AWS CLI configured with credentials for your account (`aws configure`)
- Access to S3, Kinesis, IAM, EMR Serverless, EventBridge, and CloudWatch

I set up all the AWS infrastructure manually through the console for this project — it's simpler to audit and reason about when you're learning. In a real team environment I'd use Terraform to manage all of this, but that's outside the scope here.

---

## Step 1: Create the S3 bucket and upload everything

The bucket name I'm using is `pravbala-data-engineering-projects` in `ap-south-2`. You'll need to create that first through the AWS Console if it doesn't already exist.

The folder layout inside the bucket for this project:

```
s3://pravbala-data-engineering-projects/
└── Project-2/
    ├── scripts/
    │   └── spark_aggregator.py           ← the main PySpark job
    ├── jars/
    │   ├── spark-streaming-sql-kinesis-connector_2.12-1.0.0.jar
    │   ├── hadoop-aws-3.3.4.jar
    │   └── aws-java-sdk-bundle-1.12.565.jar
    ├── sample_data_initial_load/
    │   ├── songs.csv                     ← broadcast dimension table
    │   └── users.csv                     ← broadcast dimension table
    ├── aggregations/                     ← Parquet output lands here
    └── checkpoints/                      ← Spark + Kinesis connector state
```

Upload with:

```bash
aws s3 cp scripts/spark_aggregator.py \
  s3://pravbala-data-engineering-projects/Project-2/scripts/spark_aggregator.py \
  --region ap-south-2

aws s3 cp sample_data_initial_load/songs.csv \
  s3://pravbala-data-engineering-projects/Project-2/sample_data_initial_load/songs.csv \
  --region ap-south-2

aws s3 cp sample_data_initial_load/users.csv \
  s3://pravbala-data-engineering-projects/Project-2/sample_data_initial_load/users.csv \
  --region ap-south-2

# JARs aren't in the repo (too large for GitHub — see README for download instructions)
aws s3 cp jars/spark-streaming-sql-kinesis-connector_2.12-1.0.0.jar \
  s3://pravbala-data-engineering-projects/Project-2/jars/ --region ap-south-2

aws s3 cp jars/hadoop-aws-3.3.4.jar \
  s3://pravbala-data-engineering-projects/Project-2/jars/ --region ap-south-2

aws s3 cp jars/aws-java-sdk-bundle-1.12.565.jar \
  s3://pravbala-data-engineering-projects/Project-2/jars/ --region ap-south-2
```

The `aggregations/` and `checkpoints/` prefixes don't need to be created manually — Spark creates them on the first write.

---

## Step 2: Create the Kinesis stream

Through the Kinesis console, create a stream with these settings:

- **Stream name:** `music-streams`
- **Region:** `ap-south-2`
- **Capacity mode:** Provisioned
- **Shard count:** 1

One shard is plenty for this data volume — a single shard handles up to 1,000 records/second or 1MB/second. Wait until the status shows `Active` before moving on.

---

## Step 3: Create the IAM execution role

EMR Serverless needs an IAM role to assume when running jobs. The key thing here is the trusted entity — it has to be `emr-serverless.amazonaws.com`, not Glue or EC2.

- **Role name:** `etl-project-2-emr-execution`
- **Trusted entity:** `emr-serverless.amazonaws.com`

Attach these managed policies to it:

| Policy | Why it's needed |
|--------|-----------------|
| `AmazonKinesisReadOnlyAccess` | Read records from `music-streams` |
| `AmazonS3FullAccess` | Read dimension CSVs, write Parquet output and checkpoints |
| `CloudWatchLogsFullAccess` | Stream driver and executor logs to CloudWatch |

In a real production setup I'd scope these down to specific resource ARNs — `AmazonS3FullAccess` on your whole account is too broad. But for this project it gets the job done without the overhead of writing custom inline policies.

---

## Step 4: Create the EMR Serverless application

In the EMR console → EMR Serverless → Create application:

| Setting | Value |
|---------|-------|
| Name | `etl-project-2` |
| Release label | `emr-6.15.0` (ships with Spark 3.5) |
| Application type | Spark |
| Pre-initialized capacity | Off |

Turning pre-initialized capacity off means the application truly scales to zero between job runs — workers only exist while a job is actually executing. This is what keeps costs low. The trade-off is a ~30–60s cold start on each run, which is perfectly fine given our 30-minute schedule.

Note down the **Application ID** from the console — you'll need it to submit and monitor jobs.

---

## Step 5: Fire up the producer

The producer runs directly on your Mac (not inside Docker) and pushes synthetic events to the real Kinesis stream:

```bash
export AWS_ACCESS_KEY_ID=<your-key>
export AWS_SECRET_ACCESS_KEY=<your-secret>

make aws-producer
```

Which expands to:
```bash
python3 scripts/kinesis_stream_producer.py \
  --stream-name music-streams \
  --region ap-south-2 \
  --batch-size 20 \
  --interval-seconds 5.0
  # No --local flag here — this hits real AWS
```

Let it run for a few minutes to build up some records in Kinesis before submitting the aggregator job.

---

## Step 6: Submit the EMR Serverless job

```bash
export EMR_APP_ID=<your-application-id>
export EMR_EXECUTION_ROLE_ARN=arn:aws:iam::<account-id>:role/etl-project-2-emr-execution

make emr-start
```

The full `aws emr-serverless start-job-run` command is in the Makefile — I won't repeat it in full here, but the important argument is `--trigger-mode available_now`. This is what tells Spark to use `trigger(availableNow=True)` internally, which drains all records currently sitting in Kinesis since the last checkpoint and then exits cleanly. Without this, the job would just sit idle waiting for new records forever, and you'd be paying for an idle cluster.

Check job status:
```bash
make emr-status EMR_APP_ID=<your-app-id>
```

Logs stream to CloudWatch under `/aws/emr-serverless/applications/<app-id>`. If you want to watch the driver log in near real-time, the CloudWatch console → Log groups → filter for your application ID is the easiest way.

---

## Step 7: Set up the EventBridge schedule

Manually running `make emr-start` every 30 minutes is obviously not a production workflow. Create an EventBridge Scheduler rule to automate it:

- **Schedule expression:** `rate(30 minutes)`
- **Target:** EMR Serverless → Start Job Run
- **Application ID:** your app ID from Step 4
- **Execution role:** `etl-project-2-emr-execution`
- **Job driver parameters:** same as Step 6

Once this is live, the pipeline is fully autonomous. EventBridge fires every 30 minutes, EMR spins up, processes the backlog, writes Parquet, saves the checkpoint, and dies. You get billed for maybe 2–3 minutes of actual compute per run.

---

## Step 8: Verify the output

After the first job run completes, check S3:

```bash
aws s3 ls s3://pravbala-data-engineering-projects/Project-2/aggregations/ \
  --recursive --region ap-south-2
```

You should see three table partitions, one per window that closed during the run:

```
.../hourly_streams/window_start=2026-03-07 10:00:00/part-00000-....snappy.parquet
.../top_tracks_hourly/window_start=2026-03-07 10:00:00/part-00000-....snappy.parquet
.../country_metrics_hourly/window_start=2026-03-07 10:00:00/part-00000-....snappy.parquet
```

Each partition contains a single compacted Parquet file — the `foreachBatch` + `coalesce` logic in `write_to_s3()` handles that so we don't end up with hundreds of 2KB fragments per window.

---

## Deployment summary

| Step | What | How |
|------|------|-----|
| 1 | Create S3 bucket, upload script + JARs + CSVs | AWS Console + `aws s3 cp` |
| 2 | Create Kinesis stream `music-streams` (1 shard, Provisioned) | AWS Kinesis Console |
| 3 | Create IAM execution role `etl-project-2-emr-execution` | IAM Console |
| 4 | Create EMR Serverless application `etl-project-2` | EMR Console |
| 5 | Run producer against real Kinesis | `make aws-producer` |
| 6 | Submit job run | `make emr-start` |
| 7 | Automate with EventBridge (every 30 min) | EventBridge Console |
| 8 | Verify Parquet output in S3 | `aws s3 ls ...` |

---

## Troubleshooting

The Kinesis connector has some sharp edges that took real debugging time to work through — `kinesis.endpointUrl` being required even for real AWS, `s3://` vs `s3a://` in metadata paths, concurrent queries colliding on the same metadata directory. All of it is documented in [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md). Everything in that guide applies equally to EMR Serverless and Glue — the connector behaviour is identical across both platforms.

---

## Why EMR Serverless for This Project

This project simulates a music streaming analytics pipeline with data volumes in the thousands of events, not millions. The pipeline produces windowed aggregations (hourly streams, top tracks, country metrics) with a **1-hour latency SLA** — meaning results need to be available within an hour of events being produced.

Given that context, EMR Serverless with a **30-minute EventBridge schedule** is the right fit:

| | EMR Serverless (this guide) | Glue Streaming |
|---|---|---|
| **Latency** | ~30 min (bounded by schedule) | Sub-minute (always-on) |
| **Cost** | ~$0.50–1/day | ~$21/day |
| **Compute** | Spins up on trigger, scales to zero after each run | Always running |
| **When to choose** | Latency SLA ≥ 5 min, cost matters | Latency SLA < 5 min, cost secondary to business value |

For a real production music platform processing millions of events per second where near-real-time dashboards drive business decisions, Glue Streaming (or Kinesis Data Analytics) would be justified — the cost becomes negligible relative to the business value. For this learning project, EMR Serverless keeps costs low while demonstrating the same architectural patterns.

The same `spark_aggregator.py` script runs on both — the only difference is the `--trigger-mode` argument:
- `--trigger-mode available_now` → drain Kinesis backlog then exit cleanly (EMR Serverless)
- `--trigger-mode continuous` → run forever (Glue Streaming)

---
