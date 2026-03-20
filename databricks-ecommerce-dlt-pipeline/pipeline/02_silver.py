# Databricks notebook source
"""
02_silver.py — Silver layer transformations using Delta Live Tables.

Reads from Bronze tables, applies type casting, business logic enrichment,
and data quality gates. Writes clean, typed Silver tables.

Two pairs of SCD tables are produced via dlt.apply_changes():

  Products (source: bronze_product_updates)
    silver_products              — SCD Type 1: current product state (latest wins)
    silver_product_price_history — SCD Type 2: full price change audit trail

  Customers (source: bronze_customer_updates)
    silver_customers                  — SCD Type 1: current address (latest wins)
    silver_customer_address_history   — SCD Type 2: relocation history

Both SCD Type 1 and SCD Type 2 targets are produced from the same source
stream. dlt.create_streaming_table() is required as a prerequisite
declaration before each dlt.apply_changes() call — DLT needs this to
register the target table before the CDC feed is wired up.

If-else: pipeline.mode
----------------------
The pipeline.mode configuration value controls whether Silver tables read
from Bronze using dlt.read_stream() (incremental — default) or dlt.read()
(full_refresh — re-processes all Bronze data from scratch). This is
centralised in the read_source() helper so the branching logic does not
need to be repeated in every table definition.

Data quality expectations at Silver use @dlt.expect_or_drop — rows that
fail the predicate are silently removed rather than failing the pipeline.
Invalid rows at Silver would otherwise propagate incorrect data into Gold.
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

# pipeline.mode drives streaming vs batch reads throughout Silver and Gold.
# full_refresh: re-read all Bronze data (use for backfills / schema changes).
# incremental: read only new Bronze rows since the last pipeline run (default).
pipeline_mode = spark.conf.get("pipeline.mode", "incremental")  # noqa: F821


def read_source(table_name):
    """
    Return a streaming or batch reader for the given Bronze table name,
    depending on the pipeline.mode configuration value.
    dlt.read_stream() is used for normal incremental runs.
    dlt.read() is used when pipeline.mode=full_refresh to reprocess everything.
    """
    if pipeline_mode == "full_refresh":
        return dlt.read(table_name)
    return dlt.read_stream(table_name)


# Silver: silver_order_lifecycle
#
# Joins the v_orders_created view with the first terminal event per order
# (delivered or canceled) to produce one completed lifecycle row per order.
# Watermarks on both sides bound the state size — orders that do not reach
# a terminal status within 90 days are evicted from state.
@dlt.table(
    name="silver_order_lifecycle",
    comment=(
        "One row per completed order lifecycle. Pairs the 'created' event with "
        "the terminal event (delivered or canceled) using a stream-stream join. "
        "delivery_days is NULL for canceled orders."
    ),
    table_properties={"quality": "silver", "pipelines.autoOptimize.zOrderCols": "order_id"},
)
@dlt.expect_or_drop("non_null_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("non_null_customer_id", "customer_id IS NOT NULL")
def silver_order_lifecycle():
    created = (
        dlt.read_stream("v_orders_created")
        .select(
            F.col("order_id"),
            F.col("customer_id"),
            F.col("event_timestamp").alias("created_at"),
            F.col("estimated_delivery_date"),
        )
        .withWatermark("created_at", "90 days")
    )

    terminal = (
        dlt.read_stream("bronze_order_events")
        .filter(F.col("order_status").isin("delivered", "canceled"))
        .select(
            F.col("order_id"),
            F.col("order_status").alias("final_status"),
            F.col("event_timestamp").alias("final_event_at"),
        )
        .withWatermark("final_event_at", "90 days")
    )

    # Stream-stream left outer join requires both watermarks AND an explicit time
    # range condition bounding the gap between the two sides. Without the range
    # condition Spark cannot determine when it is safe to emit a NULL right-side
    # row for an unmatched created event.
    joined = created.join(
        terminal,
        (created["order_id"] == terminal["order_id"])
        & (terminal["final_event_at"] >= created["created_at"])
        & (terminal["final_event_at"] <= created["created_at"] + F.expr("INTERVAL 90 DAYS")),
        how="left",
    )

    return joined.select(
        created["order_id"],
        "customer_id",
        "created_at",
        "final_status",
        "final_event_at",
        "estimated_delivery_date",
        F.when(
            F.col("final_status") == "delivered",
            F.datediff(F.col("final_event_at"), F.col("created_at")),
        ).alias("delivery_days"),
        F.when(
            (F.col("final_status") == "delivered")
            & (F.col("final_event_at") > F.col("estimated_delivery_date")),
            True,
        )
        .otherwise(False)
        .alias("is_late_delivery"),
    )


# Silver: silver_order_items
@dlt.table(
    name="silver_order_items",
    comment="Cleaned order line items with total_item_value (price + freight) derived.",
    table_properties={"quality": "silver", "pipelines.autoOptimize.zOrderCols": "order_id"},
)
@dlt.expect_or_drop("non_null_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("non_null_product_id", "product_id IS NOT NULL")
@dlt.expect_or_drop("valid_price", "price > 0 AND price < 50000")
@dlt.expect_or_drop("valid_freight", "freight_value >= 0")
def silver_order_items():
    return (
        read_source("bronze_order_items")
        .withColumn("price", F.col("price").cast(DoubleType()))
        .withColumn("freight_value", F.col("freight_value").cast(DoubleType()))
        .withColumn("order_item_id", F.col("order_item_id").cast(IntegerType()))
        .withColumn(
            "total_item_value",
            F.round(F.col("price") + F.col("freight_value"), 2),
        )
        .select(
            "order_id",
            "order_item_id",
            "product_id",
            "seller_id",
            "price",
            "freight_value",
            "total_item_value",
        )
    )


# Silver: silver_order_payments
@dlt.table(
    name="silver_order_payments",
    comment=(
        "Cleaned payment records. payment_type is normalised to lowercase. "
        "Rows with unrecognised payment types are dropped."
    ),
    table_properties={"quality": "silver", "pipelines.autoOptimize.zOrderCols": "order_id"},
)
@dlt.expect_or_drop("non_null_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_payment_value", "payment_value > 0")
@dlt.expect_or_drop(
    "valid_payment_type",
    "payment_type IN ('credit_card','boleto','voucher','debit_card')",
)
def silver_order_payments():
    return (
        read_source("bronze_order_payments")
        .withColumn("payment_type", F.lower(F.trim(F.col("payment_type"))))
        .withColumn("payment_value", F.col("payment_value").cast(DoubleType()))
        .withColumn("payment_installments", F.col("payment_installments").cast(IntegerType()))
        .withColumn("payment_sequential", F.col("payment_sequential").cast(IntegerType()))
        .select(
            "order_id",
            "payment_sequential",
            "payment_type",
            "payment_installments",
            "payment_value",
        )
    )


# Silver: silver_order_reviews
@dlt.table(
    name="silver_order_reviews",
    comment=(
        "Cleaned customer reviews enriched with a sentiment_category derived column. "
        "sentiment_category: scores 4-5 = positive, 3 = neutral, 1-2 = negative."
    ),
    table_properties={"quality": "silver", "pipelines.autoOptimize.zOrderCols": "order_id"},
)
@dlt.expect_or_drop("non_null_order_id", "order_id IS NOT NULL")
@dlt.expect_or_drop("valid_review_score", "review_score BETWEEN 1 AND 5")
def silver_order_reviews():
    return (
        read_source("bronze_order_reviews")
        .withColumn("review_score", F.col("review_score").cast(IntegerType()))
        .withColumn(
            "sentiment_category",
            F.when(F.col("review_score") >= 4, "positive")
            .when(F.col("review_score") == 3, "neutral")
            .otherwise("negative"),
        )
        .select("order_id", "review_id", "review_score", "sentiment_category", "review_creation_date")
    )


# SCD Type 1: silver_products
#
# Keeps only the latest known state for each product. When a price change
# event arrives in rounds 2-3, the existing row is overwritten in-place.
# Use this table when you only care about the current price or category.
#
# dlt.create_streaming_table() is required before dlt.apply_changes() so
# that DLT can register the target schema before wiring up the CDC feed.
dlt.create_streaming_table(
    name="silver_products",
    comment=(
        "SCD Type 1: current product state. Each product_id has exactly one row. "
        "Price and category are overwritten when a newer CDC event arrives."
    ),
    table_properties={"quality": "silver", "pipelines.autoOptimize.zOrderCols": "product_id"},
)

dlt.apply_changes(
    target="silver_products",
    source="bronze_product_updates",
    keys=["product_id"],
    sequence_by=F.col("updated_at"),
    stored_as_scd_type=1,
)


# SCD Type 2: silver_product_price_history
#
# Tracks every price and discount change over time. Each price change in
# rounds 2-3 creates a new history row — the previous row gets an __END_AT
# timestamp and the new row is opened with __START_AT. Use this table to
# answer questions like "what was this product's price in January 2018?"
# or "how many times did the price change before this order was placed?"
#
# track_history_column_list restricts history tracking to price and
# discount_pct only. Other product attributes (category, photo count) that
# change due to editorial corrections do not generate history rows.
dlt.create_streaming_table(
    name="silver_product_price_history",
    comment=(
        "SCD Type 2: full price change audit trail per product. "
        "DLT automatically manages __START_AT and __END_AT columns. "
        "Only price and discount_pct changes generate new history rows."
    ),
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "product_id",
    },
)

dlt.apply_changes(
    target="silver_product_price_history",
    source="bronze_product_updates",
    keys=["product_id"],
    sequence_by=F.col("updated_at"),
    stored_as_scd_type=2,
    track_history_column_list=["price", "discount_pct"],
)


# SCD Type 1: silver_customers
#
# Keeps only the latest known address for each customer. When a customer
# relocates (rounds 2-3 CDC events), their zip/city/state is overwritten.
# Use this table for shipping address lookups — only current address matters.
dlt.create_streaming_table(
    name="silver_customers",
    comment=(
        "SCD Type 1: current customer address. Each customer_id has exactly one row. "
        "Address fields are overwritten when a relocation CDC event arrives."
    ),
    table_properties={"quality": "silver", "pipelines.autoOptimize.zOrderCols": "customer_id"},
)

dlt.apply_changes(
    target="silver_customers",
    source="bronze_customer_updates",
    keys=["customer_id"],
    sequence_by=F.col("updated_at"),
    stored_as_scd_type=1,
)


# SCD Type 2: silver_customer_address_history
#
# Tracks every address a customer has been associated with over time.
# Useful for regional analysis — e.g., "did delivery satisfaction drop
# after a customer moved from SP to a state with fewer sellers?"
# Only city, state, and zip changes generate new history rows.
dlt.create_streaming_table(
    name="silver_customer_address_history",
    comment=(
        "SCD Type 2: full address relocation history per customer. "
        "DLT automatically manages __START_AT and __END_AT columns. "
        "Only changes to customer_city, customer_state, or customer_zip trigger new rows."
    ),
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.zOrderCols": "customer_id",
    },
)

dlt.apply_changes(
    target="silver_customer_address_history",
    source="bronze_customer_updates",
    keys=["customer_id"],
    sequence_by=F.col("updated_at"),
    stored_as_scd_type=2,
    track_history_column_list=["customer_city", "customer_state", "customer_zip"],
)
