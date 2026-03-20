"""
schema.py — StructType definitions for all six DLT source formats.

All Bronze tables read from JSON files uploaded to the Databricks workspace.
Schemas are intentionally kept close to the raw Olist CSV shapes to preserve
fidelity; type casting and enrichment happen in Silver.

Note: no helpers.py or validators.py in this project — DLT expectations and
declarative table definitions replace both entirely.
"""

from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

# ── Bronze: order_events ─────────────────────────────────────────────────────
# Each row represents a single order status transition.
# Produced by generate_and_upload.py which explodes Olist order timestamp
# columns into individual event rows (one per status).
#
# Fields:
#   order_id          — Olist order UUID
#   customer_id       — Olist customer UUID (links to customer_updates)
#   order_status      — one of: created | approved | shipped | delivered | canceled
#   event_timestamp   — UTC timestamp of the status transition
#   estimated_delivery_date — originally order_estimated_delivery_date from Olist;
#                             present on all rows for late-delivery detection at Gold
ORDER_EVENTS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("customer_id", StringType(), False),
        StructField("order_status", StringType(), False),
        StructField("event_timestamp", TimestampType(), False),
        StructField("estimated_delivery_date", TimestampType(), True),
    ]
)

# ── Bronze: order_items ──────────────────────────────────────────────────────
# One row per item within an order. An order can have multiple items.
# price reflects the price at time of purchase (used to derive SCD Type 2
# price history for silver_product_price_history).
#
# Fields:
#   order_id          — links to order_events
#   order_item_id     — sequential item number within the order (1, 2, 3...)
#   product_id        — links to product_updates
#   seller_id         — seller who fulfilled this item
#   price             — item price in BRL at time of purchase
#   freight_value     — shipping cost in BRL for this item
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

# ── Bronze: order_payments ───────────────────────────────────────────────────
# One row per payment installment. A single order may have multiple payment
# rows (e.g., split between credit card and voucher).
#
# Fields:
#   order_id              — links to order_events
#   payment_sequential    — installment number (1 = first payment)
#   payment_type          — credit_card | boleto | voucher | debit_card
#   payment_installments  — number of installments chosen (1 = lump sum)
#   payment_value         — BRL amount for this payment row
ORDER_PAYMENTS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("payment_sequential", IntegerType(), False),
        StructField("payment_type", StringType(), False),
        StructField("payment_installments", IntegerType(), False),
        StructField("payment_value", DoubleType(), False),
    ]
)

# ── Bronze: order_reviews ────────────────────────────────────────────────────
# Customer reviews submitted after delivery. Not all orders receive a review.
# review_score drives sentiment_category derived column in Silver.
#
# Fields:
#   order_id              — links to order_events
#   review_id             — UUID of the review
#   review_score          — integer 1–5 (1=worst, 5=best)
#   review_creation_date  — when the customer submitted the review
ORDER_REVIEWS_SCHEMA = StructType(
    [
        StructField("order_id", StringType(), False),
        StructField("review_id", StringType(), False),
        StructField("review_score", IntegerType(), False),
        StructField("review_creation_date", TimestampType(), True),
    ]
)

# ── Bronze: product_updates (CDC source) ─────────────────────────────────────
# CDC event stream for the product catalog. Every row is an insert or update.
# Round 1 contains the initial product catalog (one row per product).
# Rounds 2–3 contain price change events and category corrections for a
# subset of products — these drive both SCD Type 1 (silver_products) and
# SCD Type 2 (silver_product_price_history).
#
# Fields:
#   product_id            — Olist product UUID (CDC key)
#   product_category      — English category name (e.g., "electronics")
#   product_name_length   — number of characters in the product name
#   product_description_length — characters in the product description
#   product_photos_qty    — number of product photos
#   price                 — current listed price in BRL (tracked in SCD Type 2)
#   discount_pct          — discount percentage (0.0–1.0; tracked in SCD Type 2)
#   updated_at            — timestamp of this CDC event (SCD sequence column)
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

# ── Bronze: customer_updates (CDC source) ────────────────────────────────────
# CDC event stream for customer profiles. Every row is an insert or update.
# Round 1 contains base customer profiles (one row per customer).
# Rounds 2–3 introduce address changes for a subset of customers — these
# drive SCD Type 1 (silver_customers) and SCD Type 2
# (silver_customer_address_history, tracking city/state/zip relocations).
#
# Fields:
#   customer_id       — Olist customer UUID (CDC key)
#   customer_zip      — 5-digit Brazilian zip code (tracked in SCD Type 2)
#   customer_city     — city name (tracked in SCD Type 2)
#   customer_state    — 2-letter state code e.g. SP, RJ (tracked in SCD Type 2)
#   updated_at        — timestamp of this CDC event (SCD sequence column)
CUSTOMER_UPDATES_SCHEMA = StructType(
    [
        StructField("customer_id", StringType(), False),
        StructField("customer_zip", StringType(), True),
        StructField("customer_city", StringType(), True),
        StructField("customer_state", StringType(), True),
        StructField("updated_at", TimestampType(), False),
    ]
)
