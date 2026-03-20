# Deploying to AWS

The same `spark_aggregator.py` that runs locally works in production without any code changes — only the `--trigger-mode` argument and job parameters differ between Glue and EMR.

---

## Prerequisites

Before creating the Glue job:

**S3 bucket** — create `pravbala-data-engineering-projects` in `ap-south-2` and upload:
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

**Kinesis stream** — create `music-streams` in `ap-south-2` (Provisioned, 1 shard). Wait for status `Active`.

---

## Glue Streaming (primary deployment)

Glue Streaming keeps the job alive continuously, polling Kinesis every few seconds — sub-minute output latency, no connector compatibility issues. Cost: ~$21/day (G.1X × 2 workers).

### Step 1 — Create IAM Role for Glue

- **Role name:** `etl-project-2`
- **Trusted entity:** `glue.amazonaws.com`

Attach these policies:

| Policy | Purpose |
|--------|---------|
| `AWSGlueServiceRole` | Baseline Glue permissions |
| `AmazonKinesisReadOnlyAccess` | Read from `music-streams` |
| `AmazonS3FullAccess` | Read CSVs, write Parquet + checkpoints |
| `CloudWatchLogsFullAccess` | Stream logs |

---

### Step 2 — Create the Glue Streaming Job

**Glue console → ETL Jobs → Create job → Spark script editor**

Core settings:

| Setting | Value |
|---------|-------|
| Name | `etl-project-2-streaming` |
| Type | Spark Streaming |
| Glue version | Glue 4.0 |
| IAM Role | `etl-project-2` |
| Script location | `s3://pravbala-data-engineering-projects/Project-2/scripts/spark_aggregator.py` |
| Worker type | G.1X |
| Number of workers | 2 |
| Max concurrent runs | 1 |

**Dependent JARs** (Job details → Advanced → Dependent JARs path):
```
s3://pravbala-data-engineering-projects/Project-2/jars/hadoop-aws-3.3.4.jar,s3://pravbala-data-engineering-projects/Project-2/jars/aws-java-sdk-bundle-1.12.565.jar
```

> Glue 4.0 bundles the `aws-kinesis` connector natively — no connector JAR upload needed.

> **Note — alternative Glue-native approach (`GlueContext`):**
> Glue also provides a higher-level `GlueContext.create_data_frame.from_options(connection_type="kinesis", ...)` API that handles authentication via the attached IAM role automatically, skips the JVM credential injection, and can infer the JSON schema at runtime — making the Glue-side code considerably simpler. This project uses the lower-level `spark.readStream.format("aws-kinesis")` connector instead, so that the same `spark_aggregator.py` runs unchanged in both the local Docker environment (against LocalStack) and on Glue, with only the `--local` flag as the switch. `GlueContext` does not exist outside the Glue runtime, so using it would require a separate local code path. For a pure-production pipeline where local execution is not a requirement, `GlueContext` would be the more straightforward choice.

**Job parameters** (Job details → Advanced → Job parameters):

| Key | Value |
|-----|-------|
| `--kinesis-stream` | `music-streams` |
| `--songs-path` | `s3://pravbala-data-engineering-projects/Project-2/sample_data_initial_load/songs.csv` |
| `--users-path` | `s3://pravbala-data-engineering-projects/Project-2/sample_data_initial_load/users.csv` |
| `--output-path` | `s3://pravbala-data-engineering-projects/Project-2/aggregations` |
| `--checkpoint-path` | `s3a://pravbala-data-engineering-projects/Project-2/checkpoints` |
| `--region` | `ap-south-2` |
| `--window-minutes` | `5` |
| `--watermark-minutes` | `1` |
| `--trigger-mode` | `continuous` |

Two things to get right here:
- **`--checkpoint-path` uses `s3a://`** — Spark's `FileContext` requires it; `s3://` throws `No FileSystem for scheme: s3`.
- **`--songs-path` / `--users-path` use `s3://`** — static CSV reads go through Glue's native S3 filesystem; `s3a://` breaks them on Glue.

---

### Step 3 — Run the producer

```bash
export AWS_ACCESS_KEY_ID=<your-key>
export AWS_SECRET_ACCESS_KEY=<your-secret>

make aws-producer
```

Let it run for a minute or two before starting the Glue job.

---

### Step 4 — Start the Glue Job

**AWS Glue → ETL Jobs → `etl-project-2-streaming` → Run**

The job takes ~90s to provision the G.1X worker before any Python runs — that's normal. Once `RUNNING`, first Parquet output appears after the first window closes (~6 minutes).

---

### Step 5 — Verify output in S3

**S3 console → `pravbala-data-engineering-projects` → `Project-2/aggregations/`**

```
aggregations/
├── hourly_streams/window_start=2026-03-07 10:00:00/
├── top_tracks_hourly/window_start=2026-03-07 10:00:00/
└── country_metrics_hourly/window_start=2026-03-07 10:00:00/
```

Each folder contains one compacted Parquet file (`foreachBatch` + `coalesce(1)` handles consolidation).

---

### Step 6 — Stop the job when done

> **Don't forget this.** A G.1X × 2 worker job left running overnight burns ~$21.

**AWS Glue → ETL Jobs → `etl-project-2-streaming` → Runs tab → select run → Actions → Stop run**

---

### Deployment Summary

| Step | What | How |
|------|------|-----|
| — | Create S3 bucket, upload assets | `aws s3 cp` (see Prerequisites above) |
| — | Create Kinesis stream | Kinesis Console |
| 1 | Create IAM role `etl-project-2` | IAM Console |
| 2 | Create Glue streaming job | Glue Console |
| 3 | Run producer | `make aws-producer` |
| 4 | Start Glue job | Glue Console → Run |
| 5 | Verify S3 output | S3 Console |
| 6 | **Stop job when done** | Glue Console → Runs → Stop |

---

### Troubleshooting

See [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md) for full details. Most likely issues on Glue:

- **G1** — `SystemExit: 2`: `parse_args()` crashes on Glue-injected args — already fixed with `parse_known_args()`.
- **G2** — `kinesis.endpointUrl not specified`: connector has no default endpoint — already set explicitly in script.
- **G3** — `dotenv` import crash: `python-dotenv` absent from Glue runtime — already wrapped in `try/except`.

---

## EMR Serverless (alternative)

The same script runs on EMR Serverless with one change — swap `--trigger-mode continuous` for `--trigger-mode available_now`. Everything else (S3 paths, Kinesis stream, dimension CSVs) is identical.

On EMR, the `aws-kinesis` connector is bundled in the runtime image at `/usr/share/aws/kinesis/spark-sql-kinesis/lib/` — pass it via `--jars` with that local path when submitting (already set in `make emr-start`).

**Why it's not the primary deployment:** The awslabs connector does not implement `reportLatestOffset()`, which `Trigger.AvailableNow` requires to establish a read fence. Without it, Spark exits immediately without reading any records — the job succeeds but writes zero output. See **E2** in [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md) for the full root cause. If [Issue #79](https://github.com/awslabs/spark-sql-kinesis-connector/issues/79) is resolved upstream, EMR Serverless becomes significantly more attractive (~$0.50–1/day vs Glue's ~$21/day).

