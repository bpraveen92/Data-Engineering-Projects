# Data Engineering Projects

I've spent the past 10 years working in analytics — most recently as a Senior Business Intelligence Engineer at Amazon. Throughout that time, whenever I had the opportunity to automate a manual process, build a data feed, or enrich an existing dataset, I took it. Data engineering work has been a consistent thread through my career, even when it wasn't my primary role.

This repository is where I document and share that work in a more structured way — real-world simulated pipelines built to production patterns, with both local Docker environments and full AWS deployment journeys included. The goal isn't just to show that something works, but to show the decisions behind it: why a particular architecture, what tradeoffs I considered, and how the pipeline behaves under realistic conditions like late-arriving data, out-of-order events, and incremental processing at scale.

Each project is self-contained with its own README, runbooks, tests, and deployment documentation.

---

## Projects

### [Spotify Streams: Batch ETL Pipeline](spotify-streams-batch-etl/)
**Stack:** S3 · Airflow (MWAA) · Redshift Serverless · Great Expectations

An 8-task Airflow DAG that ingests music streaming CSV files from S3, applies data quality checks with Great Expectations, and loads curated gold-layer aggregations into Redshift Serverless on a 15-minute schedule. Built around an S3 manifest for incremental tracking so re-runs never double-count.

---

### [Spotify Streams: Real-Time Analytics Pipeline](spotify-streams-realtime-pipeline/)
**Stack:** Kinesis · PySpark Structured Streaming · AWS Glue Streaming · S3

A continuation of the batch pipeline — same Spotify dataset, different processing model. Three parallel Kinesis streams feed a PySpark Structured Streaming job that joins events with dimension tables and writes 5-minute windowed aggregations to S3 as Parquet. Built to answer the question: how much does the architecture change when you move from scheduled batch to near real-time?

---

### [Ride-Sharing Trip Lifecycle Pipeline](ridesharing-trip-lifecycle-pipeline/)
**Stack:** Kinesis · Lambda · DynamoDB · AWS Glue · Athena · LocalStack

An event-driven pipeline that joins two independent streams — trip start and trip end events from a synthetic NYC-style ride-sharing dataset — through DynamoDB. Lambda handles the stateful join per trip, stages completed trips to S3 partitioned by dropoff hour, and a checkpoint-aware Glue job runs hourly aggregations queryable through Athena. Built to handle realistic conditions: out-of-order events, missing start records, late arrivals, and incremental processing without full rescans.

---

## What's Consistent Across Projects

- **Local-first development** — every pipeline runs in Docker against LocalStack (and MinIO for Project 2) before touching AWS. `make up && make docker-all && make down` is the full local cycle.
- **Manual AWS deployment** — I set up AWS resources through the Console rather than IaC. It forces a deeper understanding of what each resource does and keeps the focus on the pipeline logic.
- **Tests included** — each project has a pytest suite covering the core logic. No mocking of the actual pipeline behaviour — only AWS calls are mocked where needed.
- **Documented decisions** — each README has a "Key Decisions" section explaining why the architecture looks the way it does, including things I tried first that didn't work.
