# Databricks notebook source
"""
01_bronze.py — Bronze layer ingestion using Delta Live Tables.

Reads raw JSON files from the Databricks workspace using cloudFiles
(Auto Loader) and materialises one Bronze table per source. All tables
are append-only at this layer — no deduplication or type casting yet.

Data quality expectations at Bronze use @dlt.expect (warn-only). Rows
are never dropped here; Bronze preserves the full raw record set so
that Silver can decide what to keep.

For-loop pattern
----------------
Order events arrive as a single stream but carry five distinct status
values (created, approved, shipped, delivered, canceled). Rather than
writing five identical @dlt.view definitions that differ only by the
filter predicate, a loop generates them. In a production pipeline with
tens of event types this pattern is essential for maintainability.

The five views (v_orders_created, v_orders_approved, v_orders_shipped,
v_orders_delivered, v_orders_canceled) are non-materialized — DLT
evaluates them inline and does not write a separate Delta table for each.
They feed directly into silver_order_lifecycle in 02_silver.py.

Source file layout (uploaded by scripts/generate_and_upload.py):
  ecommerce_data/order_events/round_{N}/data.json
  ecommerce_data/order_items/round_{N}/data.json
  ecommerce_data/order_payments/round_{N}/data.json
  ecommerce_data/order_reviews/round_{N}/data.json
  ecommerce_data/product_updates/round_{N}/data.json
  ecommerce_data/customer_updates/round_{N}/data.json
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

ORDER_EVENTS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_status", StringType(), False),
        StructField("event_timestamp", TimestampType(), False),
        StructField("estimated_delivery_date", TimestampType(), True),
    ]
)

ORDER_ITEMS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("order_item_id", IntegerType(), False),
        StructField("product_id", StringType(), False),
        StructField("seller_id", StringType(), False),
        StructField("price", DoubleType(), False),
        StructField("freight_value", DoubleType(), False),
    ]
)

ORDER_PAYMENTS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("payment_sequential", IntegerType(), False),
        StructField("payment_type", StringType(), False),
        StructField("payment_installments", IntegerType(), False),
        StructField("payment_value", DoubleType(), False),
    ]
)

ORDER_REVIEWS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("review_id", StringType(), False),
        StructField("review_score", IntegerType(), False),
        StructField("review_creation_date", TimestampType(), True),
    ]
)

PRODUCT_UPDATES_SCHEMA = StructType(
    [
        StructField("product_id", StringType(), False),
        StructField("product_category", StringType(), True),
        StructField("product_name_length", IntegerType(), True),
        StructField("product_description_length", IntegerType(), True),
        StructField("product_photos_qty", IntegerType(), True),
        StructField("price", DoubleType(), False),
        StructField("discount_pct", DoubleType(), True),
        StructField("updated_at", TimestampType(), False),
    ]
)

CUSTOMER_UPDATES_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("customer_zip", StringType(), True),
        StructField("customer_city", StringType(), True),
        StructField("customer_state", StringType(), True),
        StructField("updated_at", TimestampType(), False),
    ]
)

# Workspace path where generate_and_upload.py deposits the JSON files.
# cloudFiles (Auto Loader) monitors this path and picks up new round_N
# subdirectories automatically on each pipeline run.
data_root = spark.conf.get(  # noqa: F821 — spark is injected by the DLT runtime
    "ecommerce.data_root",
    "/Workspace/Users/pravbala30@protonmail.com/.bundle/ecommerce-dlt/dev/ecommerce_data",
)


def cloudfiles_source(table_name, schema):
    """Return a streaming cloudFiles reader for a given table subdirectory."""
    path = f"{data_root}/{table_name}/"
    return (
        spark.readStream  # noqa: F821
        .format("cloudFiles")
        .option("cloudFiles.format", "json")
        .option("cloudFiles.inferColumnTypes", "false")
        .schema(schema)
        .load(path)
    )


# Bronze: order_events
@dlt.table(
    name="bronze_order_events",
    comment="Raw order status transition events produced by exploding Olist order timestamps.",
    table_properties={"quality": "bronze", "pipelines.autoOptimize.zOrderCols": "order_id"},
)
@dlt.expect("non_null_order_id", "order_id IS NOT NULL")
@dlt.expect("non_null_customer_id", "customer_id IS NOT NULL")
@dlt.expect("valid_order_status", "order_status IN ('created','approved','shipped','delivered','canceled')")
@dlt.expect("non_null_event_timestamp", "event_timestamp IS NOT NULL")
def bronze_order_events():
    return (
        cloudfiles_source("order_events", ORDER_EVENTS_SCHEMA)
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
    )


# Bronze: order_items
@dlt.table(
    name="bronze_order_items",
    comment="Raw line items per order — one row per product per order.",
    table_properties={"quality": "bronze", "pipelines.autoOptimize.zOrderCols": "order_id"},
)
@dlt.expect("non_null_order_id", "order_id IS NOT NULL")
@dlt.expect("non_null_product_id", "product_id IS NOT NULL")
@dlt.expect("non_null_seller_id", "seller_id IS NOT NULL")
def bronze_order_items():
    return (
        cloudfiles_source("order_items", ORDER_ITEMS_SCHEMA)
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
    )


# Bronze: order_payments
@dlt.table(
    name="bronze_order_payments",
    comment="Raw payment records — one row per payment installment per order.",
    table_properties={"quality": "bronze", "pipelines.autoOptimize.zOrderCols": "order_id"},
)
@dlt.expect("non_null_order_id", "order_id IS NOT NULL")
@dlt.expect("non_null_payment_value", "payment_value IS NOT NULL")
@dlt.expect("valid_payment_type", "payment_type IN ('credit_card','boleto','voucher','debit_card')")
def bronze_order_payments():
    return (
        cloudfiles_source("order_payments", ORDER_PAYMENTS_SCHEMA)
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
    )


# Bronze: order_reviews
@dlt.table(
    name="bronze_order_reviews",
    comment="Raw customer reviews submitted after delivery. Not all orders have a review.",
    table_properties={"quality": "bronze", "pipelines.autoOptimize.zOrderCols": "order_id"},
)
@dlt.expect("non_null_order_id", "order_id IS NOT NULL")
@dlt.expect("review_score_range", "review_score BETWEEN 1 AND 5")
def bronze_order_reviews():
    return (
        cloudfiles_source("order_reviews", ORDER_REVIEWS_SCHEMA)
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
    )


# Bronze: product_updates (CDC source for SCD processing in Silver)
@dlt.table(
    name="bronze_product_updates",
    comment=(
        "CDC event stream for the product catalog. Round 1 contains the full initial catalog. "
        "Rounds 2-3 contain price change events for a subset of products. "
        "Feeds both silver_products (SCD Type 1) and silver_product_price_history (SCD Type 2)."
    ),
    table_properties={"quality": "bronze", "pipelines.autoOptimize.zOrderCols": "product_id"},
)
@dlt.expect("non_null_product_id", "product_id IS NOT NULL")
@dlt.expect("non_null_updated_at", "updated_at IS NOT NULL")
def bronze_product_updates():
    return (
        cloudfiles_source("product_updates", PRODUCT_UPDATES_SCHEMA)
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
    )


# Bronze: customer_updates (CDC source for SCD processing in Silver)
@dlt.table(
    name="bronze_customer_updates",
    comment=(
        "CDC event stream for customer profiles. Round 1 contains all base profiles. "
        "Rounds 2-3 carry address change events for customers who have relocated. "
        "Feeds both silver_customers (SCD Type 1) and silver_customer_address_history (SCD Type 2)."
    ),
    table_properties={"quality": "bronze", "pipelines.autoOptimize.zOrderCols": "customer_id"},
)
@dlt.expect("non_null_customer_id", "customer_id IS NOT NULL")
@dlt.expect("non_null_updated_at", "updated_at IS NOT NULL")
def bronze_customer_updates():
    return (
        cloudfiles_source("customer_updates", CUSTOMER_UPDATES_SCHEMA)
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_file", F.col("_metadata.file_path"))
    )


# For-loop: generate per-status order views from bronze_order_events.
#
# Five statuses share identical filter logic — only the predicate value differs.
# Generating them in a loop avoids copy-pasting five nearly identical view
# definitions and makes adding a new status a one-line change to the list.
#
# The default-argument trick (status=status) captures the loop variable by
# value at definition time, preventing all five closures from referencing
# the same final value of "status" after the loop completes.
order_statuses = ["created", "approved", "shipped", "delivered", "canceled"]

for status in order_statuses:
    @dlt.view(
        name=f"v_orders_{status}",
        comment=f"Non-materialized view of bronze_order_events filtered to status='{status}'.",
    )
    def make_status_view(status=status):
        return dlt.read_stream("bronze_order_events").filter(F.col("order_status") == status)
