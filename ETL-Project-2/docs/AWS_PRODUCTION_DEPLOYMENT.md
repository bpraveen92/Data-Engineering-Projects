# Deploying to AWS EMR Serverless — Alternative (Pending Upstream Fix)

> **Status:** EMR Serverless is documented here as a future/alternative deployment target. **It is not currently the primary deployment** — see [`GLUE_DEPLOYMENT.md`](GLUE_DEPLOYMENT.md) for the working setup.
>
> **Why EMR is blocked:** The awslabs Kinesis connector does not support `Trigger.AvailableNow` (GitHub Issue #79, open as of March 2026). This breaks the scheduled drain-and-exit architecture that makes EMR Serverless cost-effective. Specifically: the connector stores `TRIM_HORIZON` (not the actual last-read sequence number) in the Spark offset checkpoint, so cross-job resume replays from the beginning of the shard — and all replayed records are dropped as late by the watermark from the prior run. The job exits successfully but writes zero output.
>
> **When this becomes viable:** If Issue #79 is resolved upstream, EMR Serverless becomes significantly more attractive — ~$0.50–1/day vs Glue's ~$21/day at this project's scale. The script already supports `--trigger-mode available_now` and the `make emr-start` target is preserved. Switching back would require no code changes, just confirming the connector fix and deleting the stale S3 checkpoint before the first run.
>
> The full investigation and root cause are documented in [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md) (sections G5 and G6).

---

## Why EMR Serverless was the original plan

The cost argument was compelling: at ~$21/day for a Glue Streaming G.1X job vs ~$0.50–1/day for EMR Serverless running on a 30-minute EventBridge schedule, EMR would save ~$600/month at this project's scale. The architecture is clean — the job wakes up, drains the Kinesis backlog since the last checkpoint, writes Parquet to S3, and exits. Compute scales to zero between runs.

The problem is the scheduled architecture depends entirely on the connector persisting a real sequence number to the checkpoint so the next run knows where to resume. That does not work — see the [root cause section below](#the-availablenow-bug-root-cause) and `TROUBLESHOOTING.md` G5.

At a real music platform with millions of concurrent listeners, the $600/month becomes a rounding error and Glue Streaming's sub-minute output latency pays for itself. For this learning project, we use Glue Streaming because it actually works reliably.

---

## The `availableNow` bug — root cause

This section documents exactly what went wrong so that if Issue #79 is resolved upstream, you know precisely what to verify before switching to EMR.

### What should happen

`Trigger.AvailableNow` requires Spark to call `reportLatestOffset()` on the source before starting any executor work. This gives Spark a "fence" — it knows where the stream ends right now — and it processes records up to that fence, then exits cleanly. The checkpoint stores the real last-read sequence number, so the next run resumes exactly where this one left off.

### What the connector actually does

The awslabs connector implements `SupportsAdmissionControl` and provides `latestOffset(start, readLimit)`, but does **not** implement `reportLatestOffset()`. Without the fence, Spark's `AvailableNow` executor has no upper bound, decides there's nothing to process, and exits immediately. No executor partition readers run.

The connector has two checkpoint layers:

| Layer | Path | Written when | Contains |
|-------|------|-------------|----------|
| Spark `offsets/` log | `checkpoints/.../offsets/N` | Before batch N starts | Start position = `TRIM_HORIZON, iteratorPosition: ""` |
| Connector `shard-source/` | `checkpoints/.../shard-source/N/shardId-...` | After executor partition reader finishes | `AfterSequenceNumber` + real sequence number |

Since no partition readers run → `shard-source/` is never written → on the next job, `getBatchShardsInfo()` finds nothing → falls back to the `offsets/` log → reads `TRIM_HORIZON` → replays from the beginning of the shard → all records are dropped as late by the watermark that advanced in the prior run. The job exits with SUCCESS and zero output.

### Confirmed evidence (job run `00g3v9llj3al201v`, 7 March 2026)

Offset file at `s3://pravbala-de-etl-project-emr/Project-2/checkpoints/hourly_streams/offsets/1`:
```json
{"metadata":{"streamName":"music-streams","batchId":"0"},
 "shardId-000000000000":{
   "subSequenceNumber":"-1","isLast":"true",
   "iteratorType":"TRIM_HORIZON","iteratorPosition":""
 }}
```

Watermark from job 7 (last successful run): `batchWatermarkMs: 1772885321287` ≈ 2026-03-07T12:09 UTC. Any record with `event_timestamp` before that is dropped.

### What to check when Issue #79 is resolved

1. The `shard-source/` directory exists after each job run and contains `AfterSequenceNumber` entries (not `TRIM_HORIZON`).
2. The `offsets/` log stores the real sequence number, not an empty `iteratorPosition`.
3. Job N+1 processes only records newer than job N's last sequence number.
4. If all three are confirmed: remove the `--trigger-mode available_now` caveat from `Makefile` comments and update this document.

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

The bucket name I'm using is `pravbala-de-etl-project-emr` in `ap-south-2`. You'll need to create that first through the AWS Console if it doesn't already exist.

The folder layout inside the bucket for this project:

```
s3://pravbala-de-etl-project-emr/
└── Project-2/
    ├── scripts/
    │   └── spark_aggregator.py           ← the main PySpark job
    ├── jars/
    │   ├── hadoop-aws-3.3.4.jar          ← S3A filesystem support
    │   └── aws-java-sdk-bundle-1.12.565.jar
    ├── sample_data_initial_load/
    │   ├── songs.csv                     ← broadcast dimension table
    │   └── users.csv                     ← broadcast dimension table
    ├── aggregations/                     ← Parquet output lands here
    └── checkpoints/                      ← Spark streaming state
```

> **Kinesis connector:** The awslabs `spark-sql-kinesis-connector` is bundled directly in the EMR 7.1.0+ runtime image at `/usr/share/aws/kinesis/spark-sql-kinesis/lib/spark-streaming-sql-kinesis-connector.jar`. It does **not** need to be uploaded to S3 — it's referenced via its local path in the `--jars` argument when the job is submitted.

Upload with:

```bash
aws s3 cp scripts/spark_aggregator.py \
  s3://pravbala-de-etl-project-emr/Project-2/scripts/spark_aggregator.py \
  --region ap-south-2

aws s3 cp sample_data_initial_load/songs.csv \
  s3://pravbala-de-etl-project-emr/Project-2/sample_data_initial_load/songs.csv \
  --region ap-south-2

aws s3 cp sample_data_initial_load/users.csv \
  s3://pravbala-de-etl-project-emr/Project-2/sample_data_initial_load/users.csv \
  --region ap-south-2

# S3A support JARs (not in the repo — download from Maven Central or Apache)
aws s3 cp jars/hadoop-aws-3.3.4.jar \
  s3://pravbala-de-etl-project-emr/Project-2/jars/ --region ap-south-2

aws s3 cp jars/aws-java-sdk-bundle-1.12.565.jar \
  s3://pravbala-de-etl-project-emr/Project-2/jars/ --region ap-south-2
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



---

## Step 4: Create the EMR Serverless application

In the EMR console → EMR Serverless → Create application:

| Setting | Value |
|---------|-------|
| Name | `etl-project-2` |
| Release label | `emr-7.12.0` (ships with Spark 3.5, includes awslabs Kinesis connector) |
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

Manually running `make emr-start` every 30 minutes is obviously not a production workflow. EventBridge Scheduler is what makes the whole thing autonomous — it fires a `StartJobRun` API call directly against EMR Serverless on a fixed cadence, no Lambda or intermediary needed.

Go to **EventBridge → Schedules → Create schedule**:

| Setting | Value |
|---------|-------|
| Schedule name | `etl-project-2-emr-30min` |
| Schedule type | Recurring → Rate-based |
| Rate expression | `rate(30 minutes)` |
| Flexible time window | Off (run at exactly the scheduled time) |
| Target API | EMR Serverless → `StartJobRun` |
| Application ID | your app ID from Step 4 |
| Execution role (for EventBridge) | `etl-project-2-emr-execution` |
| Job driver | same JSON payload as Step 6 — paste it in directly |

The execution role needs `emr-serverless:StartJobRun` permission in addition to the S3/Kinesis/CloudWatch policies. If you're reusing `etl-project-2-emr-execution`, attach an inline policy:

```json
{
  "Effect": "Allow",
  "Action": "emr-serverless:StartJobRun",
  "Resource": "arn:aws:emr-serverless:ap-south-2:<account-id>:/applications/<app-id>"
}
```

**How the invocation chain works:**

```
EventBridge Scheduler (rate: 30 min)
        │
        │  StartJobRun API call
        ▼
EMR Serverless Application (etl-project-2)
        │
        │  spins up Spark driver + executors (~30–60s cold start)
        ▼
spark_aggregator.py --trigger-mode available_now
        │
        │  reads checkpoint → determines last committed shard offset
        │  reads Kinesis records since that offset
        │  runs aggregations → writes Parquet → saves checkpoint
        ▼
Job exits cleanly → workers deallocated → billing stops
        │
        │  30 min later
        ▼
EventBridge fires again → repeat
```

EventBridge doesn't wait for the job to finish before scheduling the next one — it fires on a wall-clock schedule regardless. If a run takes longer than 30 minutes (unlikely for this data volume, but worth knowing), the next run will start while the previous one is still going. Setting **Max concurrent runs = 1** on the EMR application prevents two jobs from fighting over the same checkpoint.

---

## Step 8: Verify the output

After the first job run completes, check S3:

```bash
aws s3 ls s3://pravbala-de-etl-project-emr/Project-2/aggregations/ \
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

## How windowing works across scheduled EMR runs

This is the part that confused me most when I first set this up, so it's worth walking through carefully. The window behaviour in EMR Serverless scheduled mode is fundamentally different from Glue's always-on continuous mode — and understanding it is key to reasoning about what output you'll see after each run.

### The setup

The pipeline uses **5-minute tumbling windows** with a **1-minute watermark**. Events carry an embedded `timestamp` field, and Spark buckets them into windows based on that event time — not the time they're processed. The watermark tells Spark to wait up to 1 minute for late-arriving records before closing a window and flushing it to output.

### What happens inside a single EMR run

Say EventBridge fires at **10:30:00** and the last checkpoint was from the **10:00:00** run. The producer has been sending events continuously for 30 minutes, so Kinesis has ~3,600 records (20 events/batch × 5s interval × 360s) sitting in the shard waiting.

When the EMR job starts:

```
Events in Kinesis shard (timestamps spread across 10:00–10:30):
  [10:00:03, 10:00:08, 10:00:14, ..., 10:29:55, 10:29:58]
        │
        │  Spark reads ALL of these in a series of micro-batches
        │  (trigger(availableNow=True) keeps reading until shard is drained)
        ▼
Micro-batch 1: reads ~200 records, timestamps mostly 10:00–10:05
Micro-batch 2: reads ~200 records, timestamps mostly 10:05–10:10
...
Micro-batch N: reads remaining records, timestamps up to 10:29:58
        │
        │  Watermark advances as event time advances
        │  Windows close once watermark passes window_end + 1 min
        ▼
Closed windows written to S3:
  window_start=10:00:00  ← all events 10:00–10:05, fully flushed
  window_start=10:05:00  ← all events 10:05–10:10, fully flushed
  window_start=10:10:00  ← ...
  window_start=10:15:00
  window_start=10:20:00
  window_start=10:25:00  ← events 10:25–10:30 (may be partial — see below)
        │
        ▼
Job exits, checkpoint saved at shard offset reached end-of-stream
```

**The last window is the tricky one.** The window `10:25:00–10:30:00` may not close in this run if the watermark hasn't advanced past `10:31:00` (window_end + watermark). Any events with timestamps in `10:25–10:30` that arrived after the last micro-batch won't be in the shard yet, so Spark can't advance its watermark far enough to flush that window confidently. It holds the state in the checkpoint and carries it forward to the next run.

### What the next run sees

EventBridge fires again at **11:00:00**. The checkpoint now contains:
- Committed shard offsets (reads from where the last run ended)
- Pending aggregation state for the `10:25:00` window

The new run picks up fresh records from Kinesis (10:30–11:00 timestamps), processes them, and the watermark now advances well past 10:31 — which finally closes and flushes the `10:25:00` window along with all the new ones.

```
Run at 10:30  →  writes windows: 10:00, 10:05, 10:10, 10:15, 10:20
                                  (10:25 stays pending in checkpoint)
Run at 11:00  →  writes windows: 10:25, 10:30, 10:35, 10:40, 10:45, 10:50, 10:55
                                  (11:25 stays pending)
Run at 11:30  →  writes windows: 11:00, 11:05, ...
```

This is correct and expected behaviour — **no data is lost or double-counted**. The checkpoint preserves the in-flight aggregation state across runs. The only observable effect is that the most recent window always appears one run late, which adds at most `schedule_interval + window_size + watermark` latency to the trailing edge — roughly 30 + 5 + 1 = **36 minutes** worst case for our config. Well within the 1-hour SLA.

### How this compares to Glue's continuous mode

In Glue, the job never exits. Spark processes micro-batches in a continuous loop:

```
Glue job running continuously:

10:00:05  micro-batch fires, reads last ~30s of records
10:00:35  micro-batch fires, reads last ~30s of records
10:01:05  micro-batch fires...
...
10:05:35  watermark passes 10:06 → window 10:00:00 closes and flushes
10:06:05  micro-batch fires...
10:10:35  watermark passes 10:11 → window 10:05:00 closes and flushes
```

Windows close roughly 1 minute after their end time (watermark = 1 min), and new Parquet partitions appear in S3 within 1–2 minutes of a window closing. That's the sub-minute-to-output latency you get with Glue Streaming.

### Side-by-side comparison

| | EMR Serverless (scheduled) | Glue Streaming (continuous) |
|---|---|---|
| **When records are read** | All at once when the job runs (batch of batches) | Continuously, every ~30s micro-batch |
| **When windows close** | When watermark advances far enough within the run | ~1 min after window end time, in real time |
| **Output latency** | Up to schedule_interval + window + watermark (~36 min) | 1–2 minutes after window closes |
| **Last window behaviour** | May appear one run late (state held in checkpoint) | Flushes as soon as watermark advances past it |
| **State across runs** | Preserved in S3 checkpoint, restored on next run | In-memory, never interrupted |
| **Data correctness** | Identical — event-time windowing, same watermark logic | Identical |

The data correctness is the same either way — Spark's event-time windowing model doesn't care whether the records arrived in one batch or as a real-time stream. The difference is purely latency and cost.

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

The main sharp edges with the awslabs Kinesis connector are documented in [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md) — specifically: `kinesis.endpointUrl` being required even for real AWS (G2), and the `ClassNotFoundException` that occurs if the connector jar isn't passed via `--jars` with its local path on the EMR image (G3). Both are already handled correctly in `make emr-start`.

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
