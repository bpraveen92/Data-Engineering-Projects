# Data Engineering Projects

I've spent the past 10 years working in the analytics and data engineering space in the US — most recently as a Senior Business Intelligence Engineer at Amazon, and before that across a range of analytics roles since completing my Master's in Computer Science in 2016. Throughout that time, I've designed and built data pipelines across a range of domains — from architecting ingestion frameworks that consolidate data from multiple upstream systems to designing the modelling and transformation layers that make that data reliable and queryable at scale. Data engineering has been a consistent thread through my career, not just as a means to an end but as the layer I find most technically interesting to get right.

I recently relocated back to India and am currently on a career break. These projects are something I built over the past few weeks — partly to stay sharp, partly because I genuinely enjoyed having uninterrupted time to build the kinds of end-to-end analytics pipelines I've always cared about: ones that pull from disparate upstream sources with different schemas, latencies, and reliability characteristics, and turn that into something clean and queryable at the other end.

Each project is self-contained with its own README, runbooks, tests, and deployment documentation.

---

## Projects

### [Ecommerce DLT Pipeline: Declarative Medallion Architecture](databricks-ecommerce-dlt-pipeline/)
**Stack:** Databricks · Delta Live Tables · PySpark · Unity Catalog · Databricks Asset Bundles

A declarative Delta Live Tables pipeline on the Olist Brazilian E-Commerce dataset. Demonstrates SCD Type 1 and Type 2 via `APPLY CHANGES INTO`, for-loop view generation, incremental vs full-refresh branching via `pipeline.mode`, and all three DLT expectation severity levels — validated with a 44-test pytest suite against live Unity Catalog tables.

---

### [F1 Intelligence: Databricks Medallion Pipeline](databricks-f1-intelligence/)
**Stack:** Databricks · Delta Lake · PySpark · Unity Catalog · Streamlit · Databricks Asset Bundles

An end-to-end analytics platform on Databricks ingesting Formula 1 data from two public APIs through a Bronze → Silver → Gold medallion architecture. Uses Delta MERGE, Change Data Feed, and Time Travel to handle late-arriving steward penalties and in-place championship standing updates — deployed via Databricks Asset Bundles with a four-page Streamlit dashboard.

---

### [Ride-Sharing Trip Lifecycle Pipeline](ridesharing-trip-lifecycle-pipeline/)
**Stack:** Kinesis · Lambda · DynamoDB · AWS Glue · Athena · LocalStack

An event-driven pipeline that joins trip start and trip end streams through DynamoDB, stages completed trips to S3, and runs hourly Glue aggregations queryable via Athena. Handles out-of-order events, missing start records, and late arrivals without full rescans.

---

### [Spotify Streams: Batch ETL Pipeline](spotify-streams-batch-etl/)
**Stack:** S3 · Airflow (MWAA) · Redshift Serverless · Great Expectations

An 8-task Airflow DAG that ingests music streaming CSV files from S3, validates with Great Expectations, and loads gold-layer aggregations into Redshift Serverless on a 15-minute schedule. Uses an S3 manifest for incremental tracking so re-runs never double-count.

---

### [Spotify Streams: Real-Time Analytics Pipeline](spotify-streams-realtime-pipeline/)
**Stack:** Kinesis · PySpark Structured Streaming · AWS Glue Streaming · S3

Same Spotify dataset as the batch pipeline, rebuilt as a streaming model. A Kinesis stream feeds a PySpark Structured Streaming job that joins events against two static dimension datasets and writes 5-minute windowed aggregations to S3 as Parquet.

---

### [NYC Taxi Analytics: dbt + Snowflake](snowflake-nyc-taxi-analytics/)
**Stack:** dbt · Snowflake · Python · Airflow · astronomer-cosmos

An analytics engineering pipeline on 24M NYC TLC Yellow Taxi trip records — the only project in this portfolio with a dedicated transformation layer. All transformation logic lives in version-controlled, tested dbt SQL models: an incremental fact table with MERGE strategy and clustering key, SCD Type 2 zone snapshot via `strategy='check'`, enforced model contract on `fct_trips`, and a custom `generate_schema_name` macro for dev/prod schema isolation. Orchestrated by a daily Airflow DAG using astronomer-cosmos, which exposes each dbt model as its own independently-retryable task.

---

## What's Consistent Across Projects

- **Local-first development** — every pipeline runs in Docker against LocalStack (and MinIO for Project 2) before touching AWS. `make up && make docker-all && make down` is the full local cycle.
- **Manual AWS deployment** — I set up AWS resources through the Console rather than IaC. It forces a deeper understanding of what each resource does and keeps the focus on the pipeline logic.
- **Tests included** — each project has a pytest suite covering the core logic. No mocking of the actual pipeline behaviour — only AWS calls are mocked where needed.
- **Documented decisions** — each README has a "Key Decisions" section explaining why the architecture looks the way it does, including things I tried first that didn't work.
