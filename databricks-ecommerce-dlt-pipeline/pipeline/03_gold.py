# Databricks notebook source
"""
03_gold.py — Gold layer aggregations using Delta Live Tables.

Reads from Silver tables and produces analytics-ready aggregations.
All Gold tables use @dlt.expect_or_fail — a data quality violation here
means something went wrong upstream, and failing fast is safer than
silently writing corrupt analytics data.

If-else: pipeline.mode
----------------------
The same read_source() helper from Silver is reproduced here. In Gold the
branching matters because:
  - full_refresh re-aggregates every Silver row from scratch (safe for backfill)
  - incremental only processes Silver rows added since the last pipeline run
    (DLT manages the watermarking and state internally)

The pipeline.mode value is set in databricks.yml per target (dev/prod) and
can be overridden at runtime via the pipeline configuration UI or CLI.

Gold tables
-----------
  gold_order_fulfillment      — one row per completed order; delivery KPIs
  gold_seller_performance     — one row per seller; aggregated across all orders
  gold_category_revenue       — one row per product category per month
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, IntegerType

pipeline_mode = spark.conf.get("pipeline.mode", "incremental")  # noqa: F821


def read_source(table_name):
    """
    Return a streaming or batch reader depending on pipeline.mode.
    full_refresh re-reads all Silver data; incremental reads only new rows.
    Centralising this logic here avoids repeating the if-else in every
    Gold table definition.
    """
    if pipeline_mode == "full_refresh":
        return dlt.read(table_name)
    return dlt.read_stream(table_name)


# Gold: gold_order_fulfillment
#
# One row per completed order. Joins the order lifecycle (delivery timing)
# with payments (total order value and payment method) and reviews (customer
# satisfaction score). LEFT JOINs are used for payments and reviews because
# not every order has both — excluding them would undercount completed orders.
#
# is_late_delivery is carried forward from silver_order_lifecycle where it
# was computed against estimated_delivery_date.
@dlt.table(
    name="gold_order_fulfillment",
    comment=(
        "One row per completed order. Captures delivery timing, total order value, "
        "primary payment method, and customer review score. "
        "is_late_delivery flags orders delivered after the estimated date."
    ),
    table_properties={"quality": "gold", "pipelines.autoOptimize.zOrderCols": "order_id"},
)
@dlt.expect_or_fail("non_null_order_id", "order_id IS NOT NULL")
@dlt.expect_or_fail("non_null_customer_id", "customer_id IS NOT NULL")
@dlt.expect_or_fail("valid_total_order_value", "total_order_value > 0")
def gold_order_fulfillment():
    lifecycle = read_source("silver_order_lifecycle").filter(
        F.col("final_status").isNotNull()
    )

    # Aggregate payments: sum all installments to get the total order value
    # and identify the primary payment method (sequential = 1)
    payments = (
        read_source("silver_order_payments")
        .groupBy("order_id")
        .agg(
            F.sum("payment_value").alias("total_order_value"),
            F.first(
                F.when(F.col("payment_sequential") == 1, F.col("payment_type"))
            ).alias("primary_payment_type"),
            F.max("payment_installments").alias("max_installments"),
        )
    )

    # Take the most recent review per order (some orders have multiple review rows)
    reviews = (
        read_source("silver_order_reviews")
        .groupBy("order_id")
        .agg(
            F.max("review_score").alias("review_score"),
            F.first("sentiment_category").alias("sentiment_category"),
        )
    )

    return (
        lifecycle
        .join(payments, on="order_id", how="left")
        .join(reviews, on="order_id", how="left")
        .select(
            "order_id",
            "customer_id",
            "final_status",
            "created_at",
            "final_event_at",
            "delivery_days",
            "is_late_delivery",
            F.round("total_order_value", 2).alias("total_order_value"),
            "primary_payment_type",
            "max_installments",
            "review_score",
            "sentiment_category",
        )
    )


# Gold: gold_seller_performance
#
# One row per seller. Aggregates across all orders fulfilled by that seller.
# DLT handles the incremental update — on each pipeline run only new
# silver_order_items rows are processed and the aggregation state is updated.
#
# Joins with silver_order_lifecycle to get delivery_days and is_late_delivery,
# and with silver_order_reviews to get average review score per seller.
# seller_id is carried from silver_order_items (each line item has a seller).
@dlt.table(
    name="gold_seller_performance",
    comment=(
        "One row per seller. Aggregated performance metrics across all fulfilled orders: "
        "total revenue, order count, average delivery time, late delivery rate, "
        "and average customer review score."
    ),
    table_properties={"quality": "gold", "pipelines.autoOptimize.zOrderCols": "seller_id"},
)
@dlt.expect_or_fail("non_null_seller_id", "seller_id IS NOT NULL")
@dlt.expect_or_fail("non_negative_total_revenue", "total_revenue >= 0")
def gold_seller_performance():
    items = read_source("silver_order_items")

    lifecycle = (
        read_source("silver_order_lifecycle")
        .select("order_id", "delivery_days", "is_late_delivery", "final_status")
        .filter(F.col("final_status") == "delivered")
    )

    reviews = (
        read_source("silver_order_reviews")
        .groupBy("order_id")
        .agg(F.avg("review_score").alias("avg_review_score"))
    )

    enriched = (
        items
        .join(lifecycle, on="order_id", how="left")
        .join(reviews, on="order_id", how="left")
    )

    return (
        enriched
        .groupBy("seller_id")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.round(F.sum("total_item_value"), 2).alias("total_revenue"),
            F.round(F.avg("price"), 2).alias("avg_item_price"),
            F.round(F.avg("delivery_days"), 1).alias("avg_delivery_days"),
            F.round(
                F.sum(F.col("is_late_delivery").cast(IntegerType()))
                / F.countDistinct("order_id")
                * 100,
                1,
            ).alias("late_delivery_rate_pct"),
            F.round(F.avg("avg_review_score"), 2).alias("avg_review_score"),
        )
    )


# Gold: gold_category_revenue
#
# Revenue and order volume aggregated per product category per calendar month.
# Joins silver_order_items (which carries product_id and price) with
# silver_products (SCD Type 1 — current category for each product) and
# silver_order_lifecycle (to get the order creation month and filter to
# delivered orders only).
#
# Using silver_products (SCD Type 1) for category lookup is intentional:
# we want to report revenue under the product's current category, not the
# category it had at the time of sale. If you need historical category
# attribution, join to silver_product_price_history instead using __START_AT
# and __END_AT to find the category active at order creation time.
@dlt.table(
    name="gold_category_revenue",
    comment=(
        "Revenue and order volume per product category per calendar month. "
        "Restricted to delivered orders. Category is resolved from silver_products "
        "(SCD Type 1 — current category). Month is derived from the order created_at timestamp."
    ),
    table_properties={"quality": "gold", "pipelines.autoOptimize.zOrderCols": "category"},
)
@dlt.expect_or_fail("non_null_category", "category IS NOT NULL")
@dlt.expect_or_fail("non_null_order_month", "order_month IS NOT NULL")
@dlt.expect_or_fail("valid_total_revenue", "total_revenue > 0")
def gold_category_revenue():
    items = read_source("silver_order_items")

    # silver_products is a SCD Type 1 table — use dlt.read() (batch) regardless
    # of pipeline.mode because SCD Type 1 targets do not support read_stream().
    products = dlt.read("silver_products").select("product_id", "product_category")

    lifecycle = (
        read_source("silver_order_lifecycle")
        .filter(F.col("final_status") == "delivered")
        .select(
            "order_id",
            F.date_format("created_at", "yyyy-MM").alias("order_month"),
        )
    )

    return (
        items
        .join(products, on="product_id", how="left")
        .join(lifecycle, on="order_id", how="inner")
        .withColumn("category", F.coalesce(F.col("product_category"), F.lit("unknown")))
        .groupBy("category", "order_month")
        .agg(
            F.countDistinct("order_id").alias("total_orders"),
            F.sum("order_item_id").alias("total_units_sold"),
            F.round(F.sum("total_item_value"), 2).alias("total_revenue"),
            F.round(F.avg("price"), 2).alias("avg_item_price"),
        )
        .orderBy("order_month", "category")
    )
