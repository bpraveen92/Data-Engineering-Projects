# Deploying to AWS Glue Streaming — Primary Deployment

> **Primary deployment target:** This is the recommended way to run this pipeline in production. AWS Glue Streaming keeps the job alive continuously, polling Kinesis every few seconds — no `availableNow` drain-and-exit, no checkpoint resume complexity, no connector bugs to work around.
>
> I originally documented EMR Serverless as the primary target because of its lower cost per run ($0.50–1/day vs Glue's ~$21/day). However, the awslabs Kinesis connector does not support `Trigger.AvailableNow` (GitHub Issue #79, open as of 2026), which breaks the cross-job checkpoint resume that makes EMR's scheduled architecture work. EMR is documented as a future alternative in [`AWS_PRODUCTION_DEPLOYMENT.md`](AWS_PRODUCTION_DEPLOYMENT.md) — if that upstream issue is resolved, the cost argument becomes compelling again.

---

## When Glue Streaming makes sense

For this project, Glue Streaming is the right fit for two reasons: it works reliably with the awslabs Kinesis connector (no `availableNow` bug), and the continuous polling model naturally handles Kinesis's event-time semantics without complex checkpoint management.

The cost comparison with EMR Serverless is worth being honest about:

| | Glue Streaming (this project) | EMR Serverless (scheduled) |
|---|---|---|
| **Daily cost** | ~$21/day (G.1X × 2 workers) | ~$0.50–1/day |
| **`availableNow` support** | N/A — runs continuously | Broken (Issue #79) |
| **Checkpoint resume** | Reliable — in-memory across batches | Broken — stores TRIM_HORIZON |
| **Kinesis connector** | Built-in to Glue runtime | Bundled on EMR image, but limited |

EMR Serverless would save ~$600/month at this scale, which is significant for a learning project. That calculation reverses at real production scale (millions of events/hour, revenue impact of stale dashboards). For now, Glue is the pragmatic choice because it works.

---

## How Glue Streaming compares to the EMR setup

It's useful to have this side by side before jumping into the steps:

| | Glue Streaming | EMR Serverless (scheduled) |
|---|---|---|
| **Job lifecycle** | Always running, never exits | Starts on EventBridge trigger, exits after draining backlog |
| **Trigger** | Manual start or Glue schedule trigger | EventBridge Scheduler every N minutes |
| **Kinesis read** | Continuous micro-batch polling — sub-minute latency | Drains backlog since last checkpoint, then stops |
| **`--trigger-mode` arg** | `continuous` (default) | `available_now` |
| **Cost model** | Per DPU-hour × 24h ≈ ~$21/day (G.1X × 2 workers) | Per vCPU-second × ~3 min/run ≈ ~$0.50–1/day |
| **Cold start** | ~90s on first run | ~30–60s per run |

---

## Prerequisites

If you've already worked through the EMR Serverless guide, you're mostly set. The S3 assets and Kinesis stream are shared:

- **S3 bucket** created and assets uploaded (script, JARs, dimension CSVs) — same as EMR Step 1.
  > **Note:** The original bucket (`pravbala-de-etl-project-emr`) has been deleted. Create a new bucket in `ap-south-1` and update the paths in `.env` (`S3_BUCKET`) and in the Glue job parameters (Step 2) before deploying.
- Kinesis stream `music-streams` active — same as EMR Step 2
- AWS CLI configured (`aws configure`)

If you're starting fresh here (not doing EMR first), go through Steps 1 and 2 in [`AWS_PRODUCTION_DEPLOYMENT.md`](AWS_PRODUCTION_DEPLOYMENT.md) first, then come back. I'd rather not repeat that setup here.

---

## Step 1: Create the IAM Role for Glue

Glue needs its own IAM role — the trusted entity is `glue.amazonaws.com`, which is different from the EMR role (`emr-serverless.amazonaws.com`), so you can't reuse `etl-project-2-emr-execution` here.

- **Role name:** `etl-project-2`
- **Trusted entity:** `glue.amazonaws.com`

Attach these policies:

| Policy | Why it's needed |
|--------|-----------------|
| `AWSGlueServiceRole` | Baseline Glue job execution permissions |
| `AmazonKinesisReadOnlyAccess` | Read records from `music-streams` |
| `AmazonS3FullAccess` | Read dimension CSVs, write Parquet output and checkpoints |
| `CloudWatchLogsFullAccess` | Stream driver and executor logs to CloudWatch |

The `AWSGlueServiceRole` managed policy handles a lot of Glue-internal permissions that are painful to replicate inline. In a real production setup I'd scope S3 down to specific bucket prefixes, but broad access is fine for a project setup.

---

## Step 2: Create the Glue Streaming Job

Go to the Glue console → ETL Jobs → Create job → Spark script editor. This is where most of the configuration lives.

**Core job settings:**

| Setting | Value |
|---------|-------|
| Name | `etl-project-2-streaming` |
| Type | Spark Streaming |
| Glue version | Glue 4.0 (Spark 3.3, Python 3) |
| IAM Role | `etl-project-2` |
| Script location | `s3://<your-s3-bucket>/Project-2/scripts/spark_aggregator.py` |
| Temporary directory | `s3://aws-glue-assets-<account-id>-ap-south-1/temporary/` |
| Worker type | G.1X |
| Number of workers | 2 |
| Max concurrent runs | 1 |

**Extra JARs** — under Job details → Advanced properties → Dependent JARs path:

```
s3://<your-s3-bucket>/Project-2/jars/hadoop-aws-3.3.4.jar,s3://<your-s3-bucket>/Project-2/jars/aws-java-sdk-bundle-1.12.565.jar
```

> **Note on the Kinesis connector for Glue:** AWS Glue 4.0 bundles the `aws-kinesis` connector natively in the runtime — no extra JAR upload is needed. The `readStream.format("aws-kinesis")` call works out of the box. The only JARs you need to supply are the S3A support libraries (`hadoop-aws` and `aws-java-sdk-bundle`) for S3A checkpoint and output paths.
>
> This is one concrete advantage Glue has over EMR Serverless for this use case: the built-in connector is well-integrated with the Glue runtime and doesn't require the ServiceLoader workaround (`--jars /usr/share/aws/kinesis/...`) that EMR needs.

**Job parameters** — under Job details → Advanced properties → Job parameters:

| Key | Value |
|-----|-------|
| `--kinesis-stream` | `music-streams` |
| `--songs-path` | `s3://<your-s3-bucket>/Project-2/sample_data_initial_load/songs.csv` |
| `--users-path` | `s3://<your-s3-bucket>/Project-2/sample_data_initial_load/users.csv` |
| `--output-path` | `s3://<your-s3-bucket>/Project-2/aggregations` |
| `--checkpoint-path` | `s3a://<your-s3-bucket>/Project-2/checkpoints` |
| `--region` | `ap-south-1` |
| `--window-minutes` | `5` |
| `--watermark-minutes` | `1` |
| `--trigger-mode` | `continuous` |

A few things worth calling out here because they're easy to get wrong:

**`--trigger-mode continuous`** keeps all three Spark streaming queries alive indefinitely, polling Kinesis every few seconds. Records land in S3 output within a minute or two of arriving in Kinesis — this is the low-latency behaviour. The flip side is the job won't stop on its own (see Step 6).

**`--checkpoint-path` uses `s3a://`** — not `s3://`. The Kinesis connector's `metadataPath` and Spark's `checkpointLocation` both go through Hadoop's `FileContext`, which requires the `s3a://` scheme. Using `s3://` here throws a `No FileSystem for scheme: s3` error at runtime. This was one of the more confusing things I hit during the original deployment.

**`--songs-path` and `--users-path` use `s3://`** — not `s3a://`. These static CSV reads go through Glue's native S3 filesystem via `spark.read.csv()`, which handles `s3://` correctly. Using `s3a://` for these would break them on Glue. So yes — the same job uses `s3://` for some paths and `s3a://` for others. It's not intuitive, but the connector forces your hand. The full explanation is in [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md).

## How windowing works in continuous mode

In Glue's `continuous` mode the job never exits, so Spark processes micro-batches on a rolling cadence rather than draining a backlog in one shot. This changes when windows close and when output appears in S3.

With our config — 5-minute windows, 1-minute watermark — here's the real-time behaviour:

```
10:00:05  micro-batch: reads ~30s of events (timestamps ~09:59–10:00)
10:00:35  micro-batch: reads ~30s of events (timestamps ~10:00)
10:01:05  micro-batch: reads ~30s of events
...
10:06:05  watermark advances past 10:06:00
          → window 10:00:00–10:05:00 closes → Parquet written to S3
10:06:35  micro-batch continues...
10:11:05  watermark advances past 10:11:00
          → window 10:05:00–10:10:00 closes → Parquet written to S3
```

Each window appears in S3 roughly **1–2 minutes after its end time**. There's no "last window" ambiguity — as long as the job is running and events keep arriving, the watermark keeps advancing and windows keep closing in near real-time.

### What this looks like in S3 over time

```
10:06  → aggregations/hourly_streams/window_start=2026-03-07 10:00:00/ appears
10:11  → aggregations/hourly_streams/window_start=2026-03-07 10:05:00/ appears
10:16  → aggregations/hourly_streams/window_start=2026-03-07 10:10:00/ appears
...    (same pattern for top_tracks_hourly and country_metrics_hourly)
```

### How this compares to EMR Serverless scheduled mode

The key difference is that EMR reads the whole 30-minute backlog in a single run, which means the watermark advances rapidly through historical event time rather than tracking real time. Windows that would take 30+ minutes to close in Glue all close within the same EMR run (except the trailing window, which carries over to the next run).

For a full walkthrough of the EMR windowing mechanics — including the trailing window behaviour, checkpoint state across runs, and a worked example — see the [How windowing works across scheduled EMR runs](AWS_PRODUCTION_DEPLOYMENT.md#how-windowing-works-across-scheduled-emr-runs) section in `AWS_PRODUCTION_DEPLOYMENT.md`.

| | Glue Streaming (this guide) | EMR Serverless (scheduled) |
|---|---|---|
| **Output latency** | ~1–2 min after window closes | Up to 36 min (schedule + window + watermark) |
| **Trailing window** | Closes naturally as new events arrive | Held in checkpoint, flushed on next run |
| **Watermark advances** | Tracks real time — 1 min per 1 min | Tracks event time — 30 min of events in minutes |
| **State continuity** | In-memory, uninterrupted | Persisted to S3 checkpoint between runs |

---

Same as the EMR setup — the producer runs on your Mac directly against the real Kinesis stream:

```bash
export AWS_ACCESS_KEY_ID=<your-key>
export AWS_SECRET_ACCESS_KEY=<your-secret>

make aws-producer
```

Which expands to:
```bash
python3 scripts/kinesis_stream_producer.py \
  --stream-name music-streams \
  --region ap-south-1 \
  --batch-size 20 \
  --interval-seconds 5.0
  # No --local flag — this targets real AWS
```

Let it run for a minute or two so there are events sitting in Kinesis before you start the Glue job.

---

## Step 4: Start the Glue Job

I kicked this off manually through the Glue console — go to **AWS Glue → ETL Jobs → `etl-project-2-streaming` → Run**. That's all it takes.

The job takes about 90 seconds to provision the G.1X worker before any Python code actually executes. That's normal for Glue, not a crash. Once it moves to `RUNNING` status in the console, keep an eye on the **Runs** tab — it shows the current state, duration, and a direct link to the CloudWatch logs if anything goes wrong.

Once it's been running for a few minutes (long enough for the first window to close), output should start appearing in S3.

---

## Step 5: Verify Output in S3

I verified this directly in the S3 console — navigate to **S3 → `<your-s3-bucket>` → `Project-2/aggregations/`**. After the first window closes you should see three partition folders appear:

```
aggregations/
├── hourly_streams/window_start=2026-03-07 10:00:00/
├── top_tracks_hourly/window_start=2026-03-07 10:00:00/
└── country_metrics_hourly/window_start=2026-03-07 10:00:00/
```

Each folder contains a single compacted Parquet file — the `foreachBatch` + `coalesce(1)` logic in `write_to_s3()` handles that consolidation so you don't end up with dozens of tiny fragments per window.

---

## Step 6: Stop the Glue Job When Done

This is the step that's easiest to forget, and forgetting it is the most common source of unexpected AWS costs. Glue streaming jobs run indefinitely — there's no `availableNow=True` drain-and-exit here. Left running overnight, a G.1X × 2 worker job burns through ~$21. Always stop it when you're done.

I stopped it through the console — **AWS Glue → ETL Jobs → `etl-project-2-streaming` → Runs tab → select the running job → Actions → Stop run**. Takes about 10 seconds to transition to `STOPPED`.

---

## Deployment Summary

| Step | What | How |
|------|------|-----|
| — | Create S3 bucket, upload assets | See `AWS_PRODUCTION_DEPLOYMENT.md` Steps 1–2 |
| 1 | Create IAM role `etl-project-2` (Glue trusted entity) | IAM Console |
| 2 | Create Glue streaming job with JAR + job parameters | Glue Console |
| 3 | Run producer against real Kinesis | `make aws-producer` |
| 4 | Start Glue job | Glue Console → Run |
| 5 | Verify Parquet output in S3 | S3 Console → browse `aggregations/` |
| 6 | **Stop Glue job when done** | Glue Console → Runs → Stop run |

S3 and Kinesis setup are shared with the EMR guide — no need to repeat them here.

---

## Troubleshooting

All the Kinesis connector related problems encountered during deployment are documented in [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md). The ones most likely to catch you on Glue:

- **G1:** Why the script uses `parse_known_args()` instead of `parse_args()` — Glue injects its own args at runtime (`--JOB_NAME`, `--JOB_RUN_ID`, `--TempDir`, etc.) that aren't in our argparse definition. `parse_args()` crashes on them. Already handled in the script, just explaining why it's there.
- **G2:** `kinesis.endpointUrl` has to be set explicitly even against real AWS — the connector doesn't default to the regional endpoint.
- **G3:** `ClassNotFoundException: aws-kinesis.DefaultSource` — this does NOT apply to Glue. The `aws-kinesis` connector is built into Glue 4.0's runtime. If you see this error on Glue, check that you're using Glue 4.0 (not an older version). On EMR Serverless, this happens if the Kinesis connector JAR isn't passed via `--jars` with the local image path — see `docs/AWS_PRODUCTION_DEPLOYMENT.md`.
- **G4:** `dotenv` import crash — `python-dotenv` isn't in the Glue runtime; the script already wraps it in `try/except`.

If you hit something not covered there, CloudWatch logs in the Glue console are the first place to look — the driver log usually tells you exactly what's failing.
