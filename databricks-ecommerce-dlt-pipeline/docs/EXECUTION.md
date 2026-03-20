# Execution Guide

## Prerequisites

- Databricks CLI installed and configured (`databricks configure`)
- Community Edition workspace at `https://dbc-7abede46-67a0.cloud.databricks.com`
- `uv` installed (`pip install uv`)

## Dataset

The JSON source files are pre-prepared and committed under `data/ecommerce_data/`. They represent the
Olist Brazilian E-Commerce dataset split into three incremental rounds:

| Round | Date Range | Orders | CDC Events |
|---|---|---|---|
| Round 1 | Sep 2016 – Dec 2017 | ~60k | Initial product catalog + customer profiles |
| Round 2 | Jan – May 2018 | ~25k | Price changes (20% of products) + address changes (15% of customers) |
| Round 3 | Jun – Aug 2018 | ~15k | Further price changes (10%) + relocations (5%) |

Before running the pipeline, upload these files to the Databricks workspace manually via the
workspace file browser:

1. Open your Databricks workspace → **Workspace** (left sidebar)
2. Navigate to (create the path if it does not exist):
   `/Workspace/Users/pravbala30@protonmail.com/.bundle/ecommerce-dlt/dev/ecommerce_data/`
3. For each table directory under `data/ecommerce_data/`, create matching subfolders and upload the
   `data.json` files maintaining the `<table>/round_N/` structure.

The final workspace structure should be:

```
ecommerce_data/
├── order_events/
│   ├── round_1/data.json
│   ├── round_2/data.json
│   └── round_3/data.json
├── order_items/
│   ├── round_1/data.json
│   ├── round_2/data.json
│   └── round_3/data.json
├── order_payments/
│   ├── round_1/data.json
│   ├── round_2/data.json
│   └── round_3/data.json
├── order_reviews/
│   ├── round_1/data.json
│   ├── round_2/data.json
│   └── round_3/data.json
├── product_updates/
│   ├── round_1/data.json
│   ├── round_2/data.json
│   └── round_3/data.json
└── customer_updates/
    ├── round_1/data.json
    ├── round_2/data.json
    └── round_3/data.json
```

## Step 1 — Deploy the DLT Pipeline

```bash
make deploy-dev
```

This deploys the DAB bundle to the dev workspace target.
The DLT pipeline `dev Ecommerce DLT Pipeline` will appear in the Databricks Pipelines UI.

## Step 2 — Run the Pipeline (Round 1 only)

Before running, ensure only round 1 files are present in the workspace. If all three rounds are
already uploaded, the pipeline will process everything in one pass — which also works fine.

To demonstrate incremental behaviour round-by-round:

1. Upload only `round_1/` files first.
2. Open **Delta Live Tables** → select `dev Ecommerce DLT Pipeline` → click **Start**.

The pipeline runs in `triggered` mode (processes all available data, then stops).

After the run completes, verify in Unity Catalog Explorer under `ecommerce_dlt.ecommerce_dev`:

| Table | Expected rows |
|---|---|
| `bronze_order_events` | ~240k |
| `bronze_order_items` | ~65k |
| `bronze_order_payments` | ~70k |
| `bronze_order_reviews` | ~55k |
| `bronze_product_updates` | ~32k |
| `bronze_customer_updates` | ~60k |
| `silver_order_lifecycle` | ~60k (one per order) |
| `silver_order_items` | ~65k |
| `silver_order_payments` | ~70k |
| `silver_order_reviews` | ~55k |
| `silver_products` | ~32k (one per product, SCD1) |
| `silver_product_price_history` | ~32k (same as round 1 base) |
| `silver_customers` | ~60k (one per customer, SCD1) |
| `silver_customer_address_history` | ~60k (same as round 1 base) |
| `gold_order_fulfillment` | ~60k |
| `gold_seller_performance` | ~3k (one per seller) |
| `gold_category_revenue` | ~few hundred (category × month) |

The Pipeline UI will show the auto-inferred lineage DAG:
`Bronze tables → v_orders_* views → Silver tables → Gold tables`

## Step 3 — Upload Round 2 and Re-Run (First Incremental)

Upload the `round_2/` files for all six tables to the workspace (same path structure as above).

In the Pipelines UI, click **Start** again. cloudFiles (Auto Loader) automatically detects the
new `round_2/` files and processes only the new data.

After the run, verify the SCD Type 2 tables have grown:

```sql
-- Price change history: products that changed price in round 2 now have 2 rows
SELECT product_id, price, discount_pct, __START_AT, __END_AT
FROM ecommerce_dlt.ecommerce_dev.silver_product_price_history
WHERE product_id IN (
  SELECT product_id FROM ecommerce_dlt.ecommerce_dev.silver_product_price_history
  GROUP BY product_id HAVING COUNT(*) > 1
)
ORDER BY product_id, __START_AT;

-- Customer relocation history: relocated customers now have 2 address rows
SELECT customer_id, customer_city, customer_state, __START_AT, __END_AT
FROM ecommerce_dlt.ecommerce_dev.silver_customer_address_history
WHERE customer_id IN (
  SELECT customer_id FROM ecommerce_dlt.ecommerce_dev.silver_customer_address_history
  GROUP BY customer_id HAVING COUNT(*) > 1
)
ORDER BY customer_id, __START_AT;
```

Gold tables update incrementally — `gold_seller_performance` now reflects round 2 orders
without re-processing any round 1 data.

## Step 4 — Upload Round 3 and Re-Run (Second Incremental)

Upload the `round_3/` files and re-run. By end of round 3, some products and customers will have
3 SCD Type 2 history rows — confirming incremental history accumulation across multiple pipeline
runs.

## Full Refresh (Backfill)

To reprocess all Bronze data from scratch (e.g., after a schema change):

1. In the Pipelines UI, click the settings gear → **Full refresh all tables**

Or trigger via CLI with `pipeline_mode=full_refresh`:

```bash
databricks bundle run ecommerce_dlt_pipeline \
  --var pipeline_mode=full_refresh \
  --target dev
```

The `read_source()` helper in `02_silver.py` detects this value via
`spark.conf.get("pipeline.mode")` and switches from `dlt.read_stream()` to `dlt.read()`
so all Silver tables are rebuilt from the complete Bronze dataset. Gold tables always use
`dlt.read()` and are re-aggregated automatically.

## Validate Bundle Configuration

```bash
make validate
```

Checks that `databricks.yml` and `resources/ecommerce_dlt.pipeline.yml` are syntactically
correct and that all referenced notebook paths exist.
