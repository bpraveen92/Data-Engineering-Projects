# Data Engineering Projects

I've spent the past 10 years working in the analytics and data engineering space in the US — most recently as a Senior Business Intelligence Engineer at Amazon, and before that across a range of analytics roles since completing my Master's in Computer Science in 2016. Throughout that time, I've designed and built data pipelines across a range of domains — from architecting ingestion frameworks that consolidate data from multiple upstream systems to designing the modelling and transformation layers that make that data reliable and queryable at scale. Data engineering has been a consistent thread through my career, not just as a means to an end but as the layer I find most technically interesting to get right.

I recently relocated back to India and am currently on a career break. These projects are something I built over the past few weeks — partly to stay sharp, partly because I genuinely enjoyed having uninterrupted time to build the kinds of end-to-end analytics pipelines I've always cared about: ones that pull from disparate upstream sources with different schemas, latencies, and reliability characteristics, and turn that into something clean and queryable at the other end.

This repository is where I document and share that work — real-world simulated pipelines built to production patterns, with both local Docker environments and full AWS deployment journeys included. The goal isn't just to show that something works, but to show the decisions behind it: why a particular architecture, what tradeoffs I considered, and how the pipeline behaves under realistic conditions like late-arriving data, out-of-order events, and incremental processing at scale.

Each project is self-contained with its own README, runbooks, tests, and deployment documentation.

---

## Projects

### [F1 Intelligence: Databricks Medallion Pipeline](databricks-f1-intelligence/)
**Stack:** Databricks · Delta Lake · PySpark · Unity Catalog · Streamlit · Databricks Asset Bundles

An end-to-end analytics platform built on Databricks that ingests Formula 1 data from two public APIs — Jolpica-F1 (race results, standings, qualifying) and OpenF1 (per-lap timing, tyre stints) — and processes it through a Bronze → Silver → Gold medallion architecture. The domain was chosen deliberately: F1 data has genuine upsert scenarios — post-race steward penalties alter results hours after a race ends, and championship standings have exactly one row per driver that gets updated in-place after every round. These are the cases where Delta MERGE, Change Data Feed, and Time Travel actually earn their place rather than being applied for show. The pipeline is deployed via Databricks Asset Bundles and serves a four-page Streamlit dashboard backed by Unity Catalog.

---

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
