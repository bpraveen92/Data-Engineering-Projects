# Architecture

## Overview

This project builds a production-grade Delta Live Tables (DLT) pipeline on the Olist Brazilian
E-Commerce dataset. It demonstrates declarative lakehouse engineering — the same medallion
architecture as the F1 Intelligence project, but expressed as `@dlt.table` declarations rather
than imperative PySpark notebooks. The key distinction is what DLT eliminates: manual CDF reads,
explicit MERGE calls, a custom checkpoint table, and custom validator classes are all replaced by
the DLT framework.

## Architecture Diagram

```mermaid
flowchart LR
    subgraph Source["Workspace — ecommerce_data/"]
        src[("JSON files\nround_1 · round_2 · round_3\n6 table directories")]
    end

    subgraph Bronze["Bronze — 01_bronze.py"]
        b_orders["bronze_order_events\n+ v_orders_* views\n(for-loop, 5 statuses)"]
        b_items["bronze_order_items\nbronze_order_payments\nbronze_order_reviews"]
        b_cdc["bronze_product_updates\nbronze_customer_updates"]
    end

    subgraph Silver["Silver — 02_silver.py"]
        s_lifecycle["silver_order_lifecycle\n(stream-stream join)"]
        s_orders["silver_order_items\nsilver_order_payments\nsilver_order_reviews"]
        s_scd["silver_products (SCD1)\nsilver_product_price_history (SCD2)\nsilver_customers (SCD1)\nsilver_customer_address_history (SCD2)"]
    end

    subgraph Gold["Gold — 03_gold.py"]
        g["gold_order_fulfillment\ngold_seller_performance\ngold_category_revenue"]
    end

    src -->|cloudFiles\nAuto Loader| b_orders & b_items & b_cdc
    b_orders --> s_lifecycle
    b_items --> s_orders
    b_cdc --> s_scd
    s_lifecycle & s_orders --> g
    s_scd -->|category lookup| g
```

## Why DLT Over Imperative Notebooks

The F1 Intelligence project uses imperative Databricks notebooks with:
- A custom `helpers.py` with `merge_delta()`, `read_incremental_or_full()`, and `save_checkpoint()`
- A `pipeline_checkpoints` Delta table to track processed versions
- Manual `enable_cdf()` and `enable_liquid_clustering()` calls after every table creation
- A `validators.py` with custom exception classes for Bronze/Silver quality checks

This project replaces all of that with DLT declarations:

| F1 (imperative) | Ecommerce DLT (declarative) |
|---|---|
| `merge_delta()` in helpers.py | `@dlt.table` — DLT materialises and updates |
| CDF reads + checkpoint table | DLT manages incremental state internally |
| `validators.py` custom classes | `@dlt.expect` / `@dlt.expect_or_drop` / `@dlt.expect_or_fail` |
| Manual Liquid Clustering calls | `pipelines.autoOptimize.zOrderCols` table property |
| `read_incremental_or_full()` | `dlt.read_stream()` vs `dlt.read()` via `read_source()` helper |
| Manual SCD ranking + MERGE | `dlt.apply_changes(stored_as_scd_type=N)` |

## DLT Patterns

### For-Loop Table Generation (01_bronze.py)

Five order status values share identical filter logic. Rather than copy-pasting five
`@dlt.view` definitions, a loop generates them from the `order_statuses` list. In a production
pipeline handling many event types this pattern is essential — adding a new status requires
only one list entry change rather than a new view definition.

```python
order_statuses = ["created", "approved", "shipped", "delivered", "canceled"]

for status in order_statuses:
    @dlt.view(name=f"v_orders_{status}")
    def make_status_view(status=status):   # default arg captures loop variable by value
        return dlt.read_stream("bronze_order_events").filter(F.col("order_status") == status)
```

### If-Else Pipeline Mode Branching (02_silver.py)

`pipeline.mode` is set in `databricks.yml` per target and can be overridden at runtime.
A single `read_source()` helper in `02_silver.py` centralises the branching for the three
straightforward Silver tables (`silver_order_items`, `silver_order_payments`,
`silver_order_reviews`). SCD tables use `dlt.apply_changes()` which manages its own
incremental state. Gold always uses `dlt.read()` — re-aggregating from the full Silver
dataset on each run is simpler and correct at that layer.

```python
pipeline_mode = spark.conf.get("pipeline.mode", "incremental")

def read_source(table_name):
    if pipeline_mode == "full_refresh":
        return dlt.read(table_name)
    return dlt.read_stream(table_name)
```

### Three Expectation Severity Levels

| Decorator | Layer | Behaviour | Rationale |
|---|---|---|---|
| `@dlt.expect` | Bronze | Warn, keep row | Raw data is raw — never drop at ingestion |
| `@dlt.expect_or_drop` | Silver | Drop invalid row silently | Bad rows must not reach Gold |
| `@dlt.expect_or_fail` | Gold | Fail the pipeline | Corrupt Gold analytics is worse than no Gold |

### SCD Type 1 and SCD Type 2 (02_silver.py)

Both patterns are demonstrated twice — once for products and once for customers —
using the same source stream as input. This illustrates a common production scenario
where you need both a current-state table and a full-history table from the same CDC feed.

**SCD Type 1** (`silver_products`, `silver_customers`): latest value wins. Use when only the
current state is relevant (e.g., shipping address lookup, current product category).

**SCD Type 2** (`silver_product_price_history`, `silver_customer_address_history`): full history
preserved with `__START_AT` and `__END_AT` columns managed by DLT. Use when historical context
matters (e.g., "what was this product's price when this order was placed?").

`track_history_column_list` restricts which columns generate new history rows — only `price`
and `discount_pct` for products, only `customer_city`, `customer_state`, and `customer_zip` for
customers. Editorial corrections to other fields (photo count, product name length) do not create
unnecessary history rows.

## Data Flow

```
Round 1 (initial load)
  data/ecommerce_data/ → upload round_1/ to workspace → pipeline run
  → 6 Bronze tables, 8 Silver tables (incl. SCD base rows), 3 Gold tables

Round 2 (first incremental)
  Upload round_2/ files to workspace → pipeline run
  → Bronze auto-picks up new files via cloudFiles
  → silver_product_price_history grows new history rows (price changes)
  → silver_customer_address_history grows new history rows (relocations)
  → Gold tables update incrementally without full recompute

Round 3 (second incremental)
  Same as round 2. Some products and customers now have 2+ SCD Type 2 history rows.
```

## Unity Catalog

| Level | Name |
|---|---|
| Catalog | `ecommerce_dlt` |
| Dev schema | `ecommerce_dev` |
| Prod schema | `ecommerce_prod` |

All table references inside pipeline notebooks use unqualified names (e.g., `bronze_order_events`)
— DLT resolves them against the catalog and target schema configured in `databricks.yml`.

## Project Structure

```
databricks-ecommerce-dlt-pipeline/
├── databricks.yml                  # DAB bundle: catalog, schema, pipeline_mode per target
├── pyproject.toml                  # dev dependencies (pytest, databricks-sdk)
├── Makefile                        # install, clean, validate, deploy-dev/prod, run-dev, test
├── pipeline/
│   ├── 01_bronze.py                # cloudFiles ingestion + for-loop status views + inline schemas
│   ├── 02_silver.py                # transformations + APPLY CHANGES INTO (SCD 1+2 pairs)
│   └── 03_gold.py                  # aggregations + batch reads at Gold layer
├── data/
│   └── ecommerce_data/             # Pre-prepared JSON dataset (committed, 3 rounds × 6 tables)
│       ├── order_events/
│       │   ├── round_1/data.json
│       │   ├── round_2/data.json
│       │   └── round_3/data.json
│       ├── order_items/            # same round_1/2/3 structure
│       ├── order_payments/         # same round_1/2/3 structure
│       ├── order_reviews/          # same round_1/2/3 structure
│       ├── product_updates/        # same round_1/2/3 structure
│       └── customer_updates/       # same round_1/2/3 structure
├── tests/
│   ├── conftest.py                 # shared fixtures: WorkspaceClient, SQL execution helper
│   ├── test_bronze.py              # 11 data quality tests (row counts, nulls, domain values)
│   ├── test_silver.py              # 18 tests (SCD1/SCD2 correctness, no duplicates, join logic)
│   └── test_gold.py                # 15 tests (aggregation accuracy, positive revenue, no duplicates)
├── resources/
│   └── ecommerce_dlt.pipeline.yml  # DLT pipeline DAB resource definition
└── docs/
    ├── ARCHITECTURE.md
    └── EXECUTION.md
```
