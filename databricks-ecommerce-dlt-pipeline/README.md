# Databricks Ecommerce DLT Pipeline

A production-grade Delta Live Tables (DLT / LakeFlow Declarative Pipelines) pipeline on the
[Olist Brazilian E-Commerce](https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce) dataset.

This project demonstrates the same medallion architecture as the
[F1 Intelligence](../databricks-f1-intelligence/) project — but expressed declaratively. Every
pattern that required custom Python in the F1 project (manual MERGE calls, a checkpoint table,
custom validator classes) is replaced by a DLT built-in.

## What This Project Demonstrates

| Pattern | Where |
|---|---|
| For-loop `@dlt.view` generation | `pipeline/01_bronze.py` |
| If-else on `pipeline.mode` via `read_source()` | `pipeline/02_silver.py`, `pipeline/03_gold.py` |
| All three expectation severity levels | Bronze (`@dlt.expect`), Silver (`@dlt.expect_or_drop`), Gold (`@dlt.expect_or_fail`) |
| SCD Type 1 with `APPLY CHANGES INTO` | `silver_products`, `silver_customers` |
| SCD Type 2 with `track_history_column_list` | `silver_product_price_history`, `silver_customer_address_history` |
| cloudFiles (Auto Loader) ingestion | All Bronze tables |
| Full refresh vs incremental via DAB variable | `var.pipeline_mode` in `databricks.yml` |

## DLT vs Imperative Comparison

| F1 Intelligence (imperative) | This project (DLT declarative) |
|---|---|
| `helpers.py` with `merge_delta()` | `@dlt.table` — DLT materialises and updates |
| CDF reads + `pipeline_checkpoints` table | DLT manages incremental state internally |
| `validators.py` custom exception classes | `@dlt.expect` / `@dlt.expect_or_drop` / `@dlt.expect_or_fail` |
| Manual `enable_liquid_clustering()` calls | `pipelines.autoOptimize.zOrderCols` table property |
| `read_incremental_or_full()` helper | `dlt.read_stream()` vs `dlt.read()` via `read_source()` |
| Manual SCD ranking + MERGE | `dlt.apply_changes(stored_as_scd_type=N)` |

## Dataset

The source data is pre-prepared and committed under `data/ecommerce_data/`. It represents the Olist
dataset split into three incremental rounds by `order_purchase_timestamp`:

- **Round 1** (Sep 2016 – Dec 2017) — initial load: ~60k orders, base product catalog, customer profiles
- **Round 2** (Jan – May 2018) — first incremental: ~25k new orders, 20% of products get price changes, 15% of customers get address changes
- **Round 3** (Jun – Aug 2018) — second incremental: ~15k new orders, further CDC events

Each round contains 6 JSON files (one per source table). Uploading all three rounds before
the first pipeline run is fine — cloudFiles processes everything. Uploading round-by-round
lets you observe incremental SCD Type 2 history accumulation step-by-step.

## Architecture

```
data/ecommerce_data/          cloudFiles         DLT Pipeline
  order_events/     →  bronze_order_events  →  v_orders_{status} (×5, for-loop)  →  silver_order_lifecycle
  order_items/      →  bronze_order_items                                          →  silver_order_items
  order_payments/   →  bronze_order_payments                                       →  silver_order_payments
  order_reviews/    →  bronze_order_reviews                                        →  silver_order_reviews
  product_updates/  →  bronze_product_updates  →  silver_products (SCD1)          →  gold_order_fulfillment
                                               →  silver_product_price_history (SCD2) →  gold_seller_performance
  customer_updates/ →  bronze_customer_updates →  silver_customers (SCD1)         →  gold_category_revenue
                                               →  silver_customer_address_history (SCD2)
```

Full architecture diagram and DLT pattern explanations: [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md)

## Project Structure

```
databricks-ecommerce-dlt-pipeline/
├── databricks.yml                  # DAB bundle: catalog, schema, pipeline_mode per target
├── pyproject.toml                  # ruff config, dev dependencies
├── Makefile                        # install, lint, fmt, validate, deploy-dev/prod
├── pipeline/
│   ├── 01_bronze.py                # cloudFiles ingestion + for-loop status views + inline schemas
│   ├── 02_silver.py                # transformations + APPLY CHANGES INTO (SCD 1+2 pairs)
│   └── 03_gold.py                  # aggregations + if-else on pipeline.mode
├── data/
│   └── ecommerce_data/                  # Pre-prepared JSON dataset (committed)
│       ├── round_1/
│       ├── round_2/
│       └── round_3/
├── resources/
│   └── ecommerce_dlt.pipeline.yml  # DLT pipeline DAB resource definition
└── docs/
    ├── ARCHITECTURE.md
    └── EXECUTION.md
```

## Quick Start

```bash
# Install dev dependencies
make install

# Validate bundle configuration
make validate

# Deploy to dev workspace
make deploy-dev
```

See [docs/EXECUTION.md](docs/EXECUTION.md) for the full step-by-step guide including workspace
file upload and incremental run instructions.

## Unity Catalog

| Level | Name |
|---|---|
| Catalog | `ecommerce_dlt` |
| Dev schema | `ecommerce_dev` |
| Prod schema | `ecommerce_prod` |

## Requirements

- Databricks workspace (Community Edition compatible)
- Databricks CLI with DAB support
- `uv` for local dependency management
- Unity Catalog enabled on the workspace
