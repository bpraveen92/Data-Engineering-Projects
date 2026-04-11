# Data Engineering Projects

I've spent the past 10 years working in the analytics and data engineering space in the US — most recently as a Senior Business Intelligence Engineer at Amazon, and before that across a range of analytics roles since completing my Master's in Computer Science in 2016. Throughout that time, I've designed and built data pipelines across a range of domains — from architecting ingestion frameworks that consolidate data from multiple upstream systems to designing the modelling and transformation layers that make that data reliable and queryable at scale. Data engineering has been a consistent thread through my career, not just as a means to an end but as the layer I find most technically interesting to get right.

I recently relocated back to India and am currently on a career break. I used the time to pick up tools I hadn't had the chance to use professionally — Databricks, dbt, and Airflow — and built these projects to put that understanding into practice and see how the pieces fit together end-to-end.

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

Two asynchronous Kinesis streams — one for trip start events, one for trip end events — each consumed by a Lambda that writes into DynamoDB. DynamoDB acts as the correlation store, matching start and end events into complete trip records which are then staged to S3. Hourly Glue PySpark jobs aggregate the staged trips into summaries queryable via Athena. Handles out-of-order events, missing start records, and late arrivals without full rescans.

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

An analytics engineering pipeline on 9.5M NYC TLC Yellow Taxi trip records — the only project in this portfolio with a dedicated transformation layer. All transformation logic lives in version-controlled, tested dbt SQL models: an incremental fact table with MERGE strategy and clustering key, SCD Type 2 zone snapshot via `strategy='check'`, enforced model contract on `fct_trips`, and a custom `generate_schema_name` macro for dev/prod schema isolation. Orchestrated by a daily Airflow DAG using astronomer-cosmos, which exposes each dbt model as its own independently-retryable task.

---

### [Portfolio RAG Assistant](portfolio-rag-assistant/)
**Stack:** Python · ChromaDB · Llama 3.3 70B (Groq) · Streamlit · Docker · Hugging Face Spaces

A RAG-based chatbot I built on top of my own portfolio documentation. I've been reading a lot about RAG, vector databases, and embeddings recently and wanted to use this as a hands-on way to see how it all fits together — chunking, embedding, retrieval, prompt construction, and generation as one end-to-end flow. Documents are split into 500-character chunks, embedded using `all-MiniLM-L6-v2` via ChromaDB's built-in ONNX function, and retrieved via cosine similarity search. Llama 3.3 70B on Groq generates answers grounded strictly in the retrieved context. Deployed as a Docker container on Hugging Face Spaces with a Streamlit chat UI.

**Live demo:** https://huggingface.co/spaces/bpraveen92/portfolio-assistant

