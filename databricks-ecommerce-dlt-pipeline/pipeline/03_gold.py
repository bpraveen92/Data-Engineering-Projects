# Databricks notebook source
"""
03_gold.py — Gold layer aggregations using Delta Live Tables.

Reads from Silver tables and produces analytics-ready aggregations.
All Gold tables use @dlt.expect_or_fail and dlt.read() (batch).

  gold_order_fulfillment  — one row per completed order; delivery KPIs
  gold_seller_performance — one row per seller; aggregated across all orders
  gold_category_revenue   — one row per product category per month
"""

import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import IntegerType


# Gold: gold_order_fulfillment
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
    lifecycle = dlt.read("silver_order_lifecycle").filter(
        F.col("final_status").isNotNull()
    )

    payments = (
        dlt.read("silver_order_payments")
        .groupBy("order_id")
        .agg(
            F.sum("payment_value").alias("total_order_value"),
            F.first(
                F.when(F.col("payment_sequential") == 1, F.col("payment_type"))
            ).alias("primary_payment_type"),
            F.max("payment_installments").alias("max_installments"),
        )
    )

    reviews = (
        dlt.read("silver_order_reviews")
        .groupBy("order_id")
        .agg(
            F.max("review_score").alias("review_score"),
            F.first("sentiment_category").alias("sentiment_category"),
        )
    )

    return (
        lifecycle
        .join(payments, on="order_id", how="inner")
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
    items = dlt.read("silver_order_items")

    lifecycle = (
        dlt.read("silver_order_lifecycle")
        .select("order_id", "delivery_days", "is_late_delivery", "final_status")
        .filter(F.col("final_status") == "delivered")
    )

    reviews = (
        dlt.read("silver_order_reviews")
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
# Category is resolved from silver_products (SCD1 — current category per product).
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
    items = dlt.read("silver_order_items")

    products = dlt.read("silver_products").select("product_id", "product_category")

    lifecycle = (
        dlt.read("silver_order_lifecycle")
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
