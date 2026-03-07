# Deploying to AWS Glue Streaming

> **Primary deployment.** Glue Streaming is the recommended production target for this pipeline. It runs continuously, handles the `aws-kinesis` connector natively (no JAR upload), and avoids the `Trigger.AvailableNow` bug that blocks EMR Serverless (Issue #79).

Cost: ~$21/day (G.1X × 2 workers). If Issue #79 is resolved upstream, EMR Serverless (~$0.50–1/day) becomes the better choice — see [`AWS_PRODUCTION_DEPLOYMENT.md`](AWS_PRODUCTION_DEPLOYMENT.md).

---

## Prerequisites

Complete Steps 1–2 from [`AWS_PRODUCTION_DEPLOYMENT.md`](AWS_PRODUCTION_DEPLOYMENT.md) first:
- S3 bucket created and assets uploaded (script, JARs, dimension CSVs)
- Kinesis stream `music-streams` active in `ap-south-2`

---

## Step 1 — Create IAM Role for Glue

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

## Step 2 — Create the Glue Streaming Job

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

## Step 3 — Run the producer

```bash
export AWS_ACCESS_KEY_ID=<your-key>
export AWS_SECRET_ACCESS_KEY=<your-secret>

make aws-producer
```

Let it run for a minute or two before starting the Glue job.

---

## Step 4 — Start the Glue Job

**AWS Glue → ETL Jobs → `etl-project-2-streaming` → Run**

The job takes ~90s to provision the G.1X worker before any Python runs — that's normal. Once `RUNNING`, first Parquet output appears after the first window closes (~6 minutes).

---

## Step 5 — Verify output in S3

**S3 console → `pravbala-data-engineering-projects` → `Project-2/aggregations/`**

```
aggregations/
├── hourly_streams/window_start=2026-03-07 10:00:00/
├── top_tracks_hourly/window_start=2026-03-07 10:00:00/
└── country_metrics_hourly/window_start=2026-03-07 10:00:00/
```

Each folder contains one compacted Parquet file (`foreachBatch` + `coalesce(1)` handles consolidation).

---

## Step 6 — Stop the job when done

> **Don't forget this.** A G.1X × 2 worker job left running overnight burns ~$21.

**AWS Glue → ETL Jobs → `etl-project-2-streaming` → Runs tab → select run → Actions → Stop run**

---

## Deployment Summary

| Step | What | How |
|------|------|-----|
| — | Create S3 bucket, upload assets | See `AWS_PRODUCTION_DEPLOYMENT.md` Steps 1–2 |
| 1 | Create IAM role `etl-project-2` | IAM Console |
| 2 | Create Glue streaming job | Glue Console |
| 3 | Run producer | `make aws-producer` |
| 4 | Start Glue job | Glue Console → Run |
| 5 | Verify S3 output | S3 Console |
| 6 | **Stop job when done** | Glue Console → Runs → Stop |

---

## Troubleshooting

See [`TROUBLESHOOTING.md`](TROUBLESHOOTING.md) for full details. Most likely issues on Glue:

- **G1** — `SystemExit: 2`: `parse_args()` crashes on Glue-injected args — already fixed with `parse_known_args()`.
- **G2** — `kinesis.endpointUrl not specified`: connector has no default endpoint — already set explicitly in script.
- **G4** — `dotenv` import crash: `python-dotenv` absent from Glue runtime — already wrapped in `try/except`.
