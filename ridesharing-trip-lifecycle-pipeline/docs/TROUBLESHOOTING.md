# Troubleshooting

Things I've run into and how I fixed them.

---

## 1) Docker stack won't start or LocalStack isn't healthy

**Symptom:** `make up` hangs, or the app container starts before LocalStack is ready and immediately fails.

**Cause:** Stale containers from a previous run, or the health check timeout was hit.

**Fix:**
```bash
make down   # force-remove everything
make up     # fresh start
```

If it still fails, check live logs:
```bash
make logs
```

Look for LocalStack startup errors — usually a port conflict or a previous process still holding `:4566`.

---

## 2) `ResourceNotFoundException` when consuming Kinesis streams

**Symptom:** Producer or runner fails with `ResourceNotFoundException: Requested resource not found`.

**Cause:** The producer ran before `setup_local.py` created the streams, or the streams were created in a different region than what the scripts are using.

**Fix:**
```bash
make docker-setup-local   # recreate resources
make docker-producer      # re-publish
```

---

## 3) Lambda can't reach DynamoDB or S3

**Symptom:** Lambda logs show `EndpointResolutionError` or connection refused.

**Cause:** Endpoint mismatch. Locally, `AWS_ENDPOINT_URL` must point to LocalStack. In AWS, it must be empty (or unset) so boto3 uses the real endpoint.

**Fix for local:** Confirm `AWS_ENDPOINT_URL=http://etl-project-3-localstack:4566` is set in `docker-compose.yml` for the app service.

**Fix for AWS:** Make sure no `DYNAMODB_ENDPOINT_URL` or `AWS_ENDPOINT_URL` env var is set on the Lambda function.

---

## 4) Too many `trip_end_upsert` / `missing_start` records

**Symptom:** Glue aggregation output has fewer rows than expected, or DynamoDB has many items where `data_quality=missing_start`.

**Cause:** Trip end events were processed before the corresponding start events. This is expected during interleaved publishing — the producer publishes both streams concurrently, so order isn't guaranteed.

**What happens internally:** `lambda_trip_end` checks DynamoDB for an existing item keyed by `trip_id`. If it doesn't find start-side fields, it marks `data_quality=missing_start` and skips the S3 staging write. These trips never reach Glue.

**To check how many:**
```bash
aws dynamodb scan \
  --table-name "$TRIP_LIFECYCLE_TABLE" \
  --region "$AWS_REGION" \
  --filter-expression "data_quality = :v" \
  --expression-attribute-values '{":v":{"S":"missing_start"}}' \
  --select COUNT
```

**If the count is higher than expected:** re-run the producer and runner — most trips will resolve once both events have been processed.

---

## 5) Glue job runs but writes zero output rows

**Symptom:** Glue run succeeds (`SUCCEEDED` status) but `metric_rows=0` in the summary log.

**Causes I've seen:**
- All staged trips have `data_quality=missing_start` (filtered in `deduplicate_completed_trips`)
- No staged files are newer than the checkpoint — run already processed everything
- The hour partitions in all staged files are beyond the 60-minute lateness cap

**How to check:**
```bash
aws glue get-job-run \
  --job-name "$GLUE_JOB_NAME" \
  --run-id "<run-id>" \
  --region "$AWS_REGION"
```

Check the CloudWatch logs for that run — look for:
```
No new staged completed-trip files found after checkpoint. Glue run will exit cleanly.
```
or:
```
Dropped N staged file(s) beyond 60-minute lateness cap
```

**Fix:** Either re-run the producer to generate fresh staged files, or delete the checkpoint to force a full reprocess:
```bash
aws s3 rm "s3://<your-bucket>/checkpoints/hourly_zone_metrics_checkpoint.json" \
  --region "$AWS_REGION"
```

---

## 6) Events dropped by the 60-minute late arrival policy

**Symptom:** Glue logs show `Dropping late-arriving staged file` and the expected trip counts are lower than what's in S3 staging.

**This is intentional behavior.** I enforce a strict 60-minute window: if a staged file's hour partition closed more than 60 minutes ago, it's dropped. The reference point is `hour_end` (e.g., `hour=14` ends at `15:00`), so an event with `dropoff_datetime=14:59` isn't penalized for arriving shortly after 15:00.

Example log output:
```
INFO  Dropping late-arriving staged file: key=staging/.../hour=12/batch.jsonl hour=2024-05-25 12 closed 150.0 min ago (cap=60 min)
WARN  Dropped 3 staged file(s) beyond 60-minute lateness cap
```

**If you need to reprocess dropped events** (for backfilling): temporarily delete the checkpoint and run Glue while the files are still accessible. The lateness check is based on `datetime.utcnow()` at runtime, so old test data from the sample CSVs will always be beyond the window when Glue actually runs in AWS.

**For local testing** with the sample data: the test events are timestamped `2024-05-25`, which is months in the past — they'd all be dropped by the live lateness check. That's why `local_pipeline_runner.py` uses `local_staging` mode, which calls the aggregation differently (the lateness window is evaluated against the current clock, but the S3 staging happens instantly so the file `LastModified` timestamps are fresh).

---

## 7) Glue output has stale data in Athena after a new run

**Symptom:** Athena still returns old row counts even after a successful Glue run.

**Cause:** The Glue crawler hasn't run yet to register new partitions in the catalog.

**Fix:**
```bash
aws glue start-crawler \
  --name etl-project-3-trip-metrics-crawler \
  --region "$AWS_REGION"
```

Wait for it to complete (usually 1–2 minutes), then re-run your Athena query.

---

## 8) Lambda code loads but handler errors in Console test

**Symptom:** Lambda test runs but returns a 500 or Python traceback.

**Causes I've seen:**
- Pasted code into the wrong file (not `lambda_function.py`)
- Handler field still set to `lambda_function.lambda_handler` instead of `lambda_function.handler`
- Forgot to click **Deploy** after pasting

**Fix:**
1. Make sure all code is in `lambda_function.py` in the editor
2. Set handler to `lambda_function.handler`
3. Click **Deploy**
4. Run a quick test with `{"Records": []}` — should return `{"statusCode": 200, "body": "..."}` cleanly

---

## 9) Checkpoint gets corrupted or points to a deleted file

**Symptom:** Glue fails with a boto3 error trying to read the checkpoint, or incremental processing seems to skip large amounts of data.

**Fix:** Just delete the checkpoint and let the next Glue run rebuild it from scratch:
```bash
aws s3 rm "s3://<your-bucket>/checkpoints/hourly_zone_metrics_checkpoint.json" \
  --region "$AWS_REGION"
```

The next run will process all staged files and write a fresh checkpoint at the end. This is safe because `write_output` uses `dynamic` partition overwrite — reprocessing the same data just overwrites the same Parquet partitions.
