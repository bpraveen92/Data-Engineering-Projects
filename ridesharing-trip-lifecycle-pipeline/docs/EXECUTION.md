# Execution Guide

My runbook for running the pipeline locally and on AWS. Local first, always.

---

## A) Local (Docker)

Typical runtimes on my machine:

| Command | Time |
|---|---|
| `make up` (first build) | 5–8 min |
| `make up` (cached) | 30–90 sec |
| `make docker-setup-local` | 20–40 sec |
| `make docker-producer` | 60–120 sec |
| `make docker-runner` | 6–10 min |
| `make down` | 10–30 sec |

### Step 1 — Start the stack

```bash
make up
```

What I expect to see:
```
Container etl-project-3-localstack  Created
Container etl-project-3-localstack  Started
Container etl-project-3-localstack  Healthy
Container etl-project-3-app         Started
```

The `Healthy` line is the important one — LocalStack runs a health check and the app container waits for it before starting.

### Step 2 — Create local resources

```bash
make docker-setup-local
```

What I expect to see:
```
INFO  Creating Kinesis stream: trip-start-stream
INFO  Creating Kinesis stream: trip-end-stream
INFO  Creating DynamoDB table: trip_lifecycle
INFO  Creating S3 bucket: etl-project-3-analytics-local
WARN  Skipping Glue database check: glue service not in LocalStack community license
INFO  Local setup complete.
```

After this I have:
- Two Kinesis streams (`trip-start-stream`, `trip-end-stream`) in LocalStack
- DynamoDB table `trip_lifecycle` with `trip_id` as the partition key
- S3 bucket `etl-project-3-analytics-local` for staging + checkpoint + output

The Glue database warning is expected — LocalStack community doesn't emulate Glue. The database gets created in AWS later.

### Step 3 — Publish events to Kinesis

```bash
make docker-producer
```

What I expect to see:
```
INFO  Starting publish: start_rows=4999 end_rows=4999 batch_size=100
INFO  trip_start → trip-start-stream  sent=100 total=100
INFO  trip_end   → trip-end-stream    sent=100 total=100
...
INFO  trip_start → trip-start-stream  sent=99  total=4999
INFO  trip_end   → trip-end-stream    sent=99  total=4999
INFO  Publishing complete. start=4999 end=4999
```

This reads both CSVs from `data/` and pushes them to their respective streams in interleaved batches of 100. The interleaving intentionally mixes start and end events so the Lambda handlers see out-of-order arrivals — same as real traffic.

### Step 4 — Process streams and aggregate

```bash
make docker-runner
```

This is the main step. It does four things in sequence:
1. Reads all records from both Kinesis streams
2. Feeds batches into `lambda_trip_start.handler` → writes start state to DynamoDB
3. Feeds batches into `lambda_trip_end.handler` → updates DynamoDB, appends JSONL to S3 staging
4. Calls `run_local_staging_aggregation` → reads new staged files, runs Spark transforms, writes Parquet

What I expect to see:
```
INFO  Fetched stream records: trip_start=4999 trip_end=4999
INFO  trip_start batch result={'processed': 100, 'failed': 0, 'table': 'trip_lifecycle'}
WARN  Skipping invalid trip_end event; missing=['rate_code', 'passenger_count'] payload={...}
INFO  trip_end batch result={'processed': 92, 'failed': 8, 'staged_completed_trips': 92, ...}
...
INFO  trip_start summary={'processed': 4999, 'failed': 0}
INFO  trip_end summary={'processed': 4468, 'failed': 531, 'staged_completed_trips': 4468}
INFO  Local incremental aggregation completed summary={
        'metric_rows': 3460,
        'affected_hours': 25,
        'top_route_rows': 0,
        'new_objects': 1145
      }
INFO  Local pipeline completed. Output path=/workspace/output/aggregations
```

- `failed=531` on trip_end is expected — those rows in `trip_end.csv` have missing required fields (rate_code, etc.)
- `staged_completed_trips=4468` means 4468 completed trips passed validation and were written to S3 staging
- `affected_hours=25` means Glue rewrote 25 hour partitions this run
- `new_objects=1145` is how many staged JSONL files the checkpoint hadn't seen yet

### Step 5 — Validate output

List the output partitions:
```bash
find output/aggregations -type f | sort
```

Expected structure:
```
output/aggregations/year=2024/month=05/day=25/hour=00/part-0.parquet
output/aggregations/year=2024/month=05/day=25/hour=01/part-0.parquet
...
output/aggregations/year=2024/month=05/day=26/hour=00/part-0.parquet
```

25 partitions total — 24 hours of 2024-05-25 plus one spillover into 2024-05-26 hour=00 from late drop-offs.

Validate DynamoDB data by reviewing the Parquet aggregation output — 25 partitions with expected row counts confirms the join worked. The 531 `missing_end` items are trips where the `trip_end` event arrived before `trip_start` — they exist in DynamoDB with only start-side fields and are intentionally excluded from aggregation.

### Step 6 — Optional: top routes output

```bash
export TOP_ROUTES_OUTPUT_PATH=./output/top_routes_hourly
export TOP_ROUTES_LIMIT=5
make docker-runner
```

This adds a second Parquet dataset with per-hour route rankings:
- `route_rank_in_hour` — ranked by trip count, then fare_sum within each hour
- `avg_fare_per_trip`, `avg_tip_per_trip`, `tip_to_fare_ratio`

Sample row from my last run:
```
event_hour=2024-05-25 00:00:00  pickup=82  dropoff=138  trip_count=3  rank=1  avg_fare=69.27  tip_ratio=0.021
```

### Step 7 — Stop

```bash
make down
```

Expected:
```
Container etl-project-3-app        Removed
Container etl-project-3-localstack  Removed
Network etl-project-3_etl-network   Removed
```

### One-command run

If I just want to run the whole thing without thinking about steps:
```bash
make up && make docker-all && make down
```

---

## B) AWS Execution

### Step 1 — Configure environment

```bash
cp .env.example .env
# fill in: AWS_REGION, AGGREGATION_BUCKET, TRIP_LIFECYCLE_TABLE, etc.
```

### Step 2 — Create AWS prerequisites

I follow the Console setup checklist in [`docs/AWS_DEPLOYMENT.md`](AWS_DEPLOYMENT.md) in this order:
1. S3 bucket + upload Glue script
2. Kinesis streams
3. DynamoDB table
4. IAM roles
5. Lambda functions + Kinesis triggers
6. Glue job + EventBridge schedule
7. Glue database + crawlers
8. Athena output location

### Step 3 — Publish sample events

```bash
python3 scripts/kinesis_trip_producer.py \
  --region "$AWS_REGION"
```

(No `--endpoint-url` needed for AWS — the script defaults to the real AWS endpoint.)

### Step 4 — Check Lambda is processing

CloudWatch → Log groups → `/aws/lambda/etl-project-3-trip-end`

What I look for:
```
trip_end batch result={'processed': 92, 'staged_completed_trips': 92, 'failed': 0, ...}
```

Validate DynamoDB data via the AWS Console: open the `trip_lifecycle` table → **Explore items** and spot-check a few rows. Items that have both `start_pickup_location_id` and `end_dropoff_datetime` confirm the join worked.

### Step 5 — Wait for (or force) the Glue run

**Lambda does not trigger Glue.** After Lambda finishes processing, it's done — it has written to DynamoDB and written completed trips to S3 staging. That's it. Glue is on a separate EventBridge hourly schedule (`cron(0 * * * ? *)`), so it won't run until the next hour boundary.

For a demo or ad-hoc test, force a run now instead of waiting:

```bash
aws glue start-job-run \
  --job-name "$GLUE_JOB_NAME" \
  --region "$AWS_REGION"
```

Then poll until it finishes (usually 60–90 seconds for the sample dataset):

```bash
aws glue get-job-runs \
  --job-name "$GLUE_JOB_NAME" \
  --region "$AWS_REGION" \
  --max-results 3
```

I want to see `JobRunState: SUCCEEDED`. If it's still `RUNNING`, just wait and check again.

### Step 6 — Refresh Athena catalog

```bash
aws glue start-crawler \
  --name etl-project-3-trip-metrics-crawler \
  --region "$AWS_REGION"
```

Then run queries from `artifacts/sql/athena_queries.sql`.

---

## End-to-End Walkthrough: First Run and Beyond

**Setup:** Lambda, Kinesis, and DynamoDB were deployed 2 days ago and have been running. Completed trips have been streaming into `staging/completed_trips/` the whole time — Lambda writes one JSONL file per invocation, partitioned by `dropoff_datetime` hour. A busy hour might accumulate dozens of files:

```
staging/completed_trips/
  year=2025/month=03/day=14/hour=00/
    completed_trip_batch_20250314T000312Z_<uuid>.jsonl
    completed_trip_batch_20250314T000847Z_<uuid>.jsonl
    ...  (one file per Lambda invocation)
  ...  (48 hours worth of partitions)
  year=2025/month=03/day=16/hour=13/
    completed_trip_batch_20250316T130201Z_<uuid>.jsonl
    completed_trip_batch_20250316T135822Z_<uuid>.jsonl
    ...
```

Today the Glue job is deployed. No checkpoint exists yet.

---

### First run — 2025-03-16 14:00 UTC

No checkpoint → full scan of the entire staging tree, lateness cap disabled. Every partition across 2 days is included. Glue reads all JSONL files per hour, deduplicates by `trip_id`, and writes one Parquet file per hour partition. Then it writes the checkpoint:

```json
{
  "last_modified": "2025-03-16T13:58:22+00:00",
  "last_key": "staging/.../hour=13/completed_trip_batch_20250316T135822Z_<uuid>.jsonl",
  "processed_object_count": 847
}
```

The checkpoint marks the `LastModified` + S3 key of the most recently seen staged file.

---

### Second run — 2025-03-16 15:05 UTC

Checkpoint exists → targeted listing + 60-minute lateness cap active.

Glue lists only the 2 most recently closed hour prefixes — the currently-open hour is skipped since it has barely any data and will be fully captured next run:

```
floor(15:05) - 2h → hour=13  (closed 65 min ago)
floor(15:05) - 1h → hour=14  (closed 5 min ago)
                    hour=15  ← skipped (still open)
```

From those 2 prefixes, only files newer than the checkpoint are picked up. Any hour prefix that has at least one new file gets fully re-aggregated — both `hour=13` and `hour=14` are processed if they have new files, regardless of how many minutes ago they closed. The 2-hour listing window is the only lateness gate; there's no additional age check on top.

| Hour | New files since checkpoint? | Decision |
|---|---|---|
| `hour=13` | yes (late arrivals landed here) | ✓ re-aggregated |
| `hour=14` | yes (normal traffic) | ✓ aggregated |

For each included prefix, Glue reads **all** files in that directory (not just the new ones), aggregates into one Parquet partition, and writes it. Checkpoint advances to the latest file seen across both prefixes.

---

### Late arrivals

The staging partition is always keyed by the trip's `dropoff_datetime`, not when Lambda ran. A trip with `dropoff_datetime=13:47` that Lambda processes at 14:45 lands in `staging/.../hour=13/` — not `hour=14/`. This keeps late-arriving events in the right hour bucket.

**Within the window:** that 13:47 trip arrives at 14:45 → file lands in `staging/.../hour=13/` (keyed by `dropoff_datetime`). At the **15:05** Glue run, `hour=13` is one of the 2 listed prefixes and has new files → re-aggregated. Parquet for `hour=13` is rewritten with this trip counted in. ✓

**Outside the window:** same trip but Lambda doesn't process it until 16:15 → file again lands in `staging/.../hour=13/`. At the **16:05** Glue run, the 2 listed prefixes are now `hour=14` and `hour=15` — `hour=13` is no longer in the listing window and is never listed. The trip lands in DynamoDB but never reaches Parquet or Athena.

---

### Reprocessing after a gap

Delete the checkpoint — the next run treats it as a first run, full scan, no cap:

```bash
aws s3 rm "s3://<bucket>/checkpoints/hourly_zone_metrics_checkpoint.json" --region "$AWS_REGION"
```

Safe to run multiple times — dynamic partition overwrite means reprocessing just overwrites the same Parquet partitions.
