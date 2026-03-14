# Troubleshooting

These are the issues I’d check first if the pipeline doesn’t behave as expected.

## 1) Docker stack does not start cleanly

Cause I usually see:
- LocalStack container has stale state or a previous run did not shut down cleanly.

Fix I use:

```bash
make down
make up
```

If needed, I also check live logs:

```bash
make logs
```

## 2) `ResourceNotFoundException` while consuming streams

Cause I usually see:
- Producer ran before streams existed.

Fix I use:

```bash
make docker-setup-local
make docker-producer
```

## 3) Lambda can’t reach DynamoDB

Cause I usually see:
- Endpoint mismatch between local vs AWS values.

Fix I use:
- For local, confirm `AWS_ENDPOINT_URL` and `DYNAMODB_ENDPOINT_URL` are `http://localhost:4566`.
- For AWS, clear local endpoint values.

## 4) Too many `trip_end_upsert` records

Cause I usually see:
- End events were processed before start events.

Expected behavior:
- Lambda sets `record_source=trip_end_upsert` and `data_quality=missing_start`.

Validation query:

```bash
aws dynamodb scan \
  --table-name "$TRIP_LIFECYCLE_TABLE" \
  --region "$AWS_REGION" \
  --filter-expression "data_quality = :v" \
  --expression-attribute-values '{":v":{"S":"missing_start"}}'
```

## 5) Glue job runs but writes no output rows

Cause I usually see:
- Trips are not marked `completed`, or
- Most rows are `data_quality=missing_start` and get filtered out.

Fix I use:
- Sample DynamoDB items and confirm both start + end fields exist.
- Re-run producer to ensure both streams were populated.

## 6) Athena table has no rows after Glue run

Cause I usually see:
- Crawler was not run after new partitions were written.

Fix I use:

```bash
aws glue start-crawler --name etl-project-3-trip-metrics-crawler --region "$AWS_REGION"
```

## 7) Lambda handler error after manual code paste in Console

Cause I usually see:
- I pasted code into `lambda_function.py` but handler is still set to some other module name.

Fix I use:
- Keep handler as `lambda_function.handler` for both Lambda functions.
- Click `Deploy` after each code update.
- Run a quick test with `{"Records":[]}` to confirm the handler loads.
