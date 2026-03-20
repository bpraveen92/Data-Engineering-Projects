# Databricks notebook source
"""
02_silver.py — Silver layer transformations using Delta Live Tables.

Reads from Bronze tables, applies type casting, business logic enrichment,
and data quality gates. Expectations use @dlt.expect_or_drop.

SCD tables via dlt.apply_changes():
  bronze_product_updates  → silver_products (SCD1) + silver_product_price_history (SCD2)
  bronze_customer_updates → silver_customers (SCD1) + silver_customer_address_history (SCD2)

read_source() switches between dlt.read_stream() (incremental) and dlt.read()
(full_refresh) based on pipeline.mode.
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType, TimestampType

pipeline_mode = spark.conf.get("pipeline.mode", "incremental")


def read_source(table_name):
    """Return a streaming or batch reader depending on pipeline.mode."""
    if pipeline_mode == "full_refresh":
        return dlt.read(table_name)
    return dlt.read_stream(table_name)


# Silver: silver_order_lifecycle
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
# track_history_column_list restricts history rows to price and discount_pct changes only.
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
# track_history_column_list restricts history rows to city, state, and zip changes only.
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
