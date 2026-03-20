"""
test_silver.py — Data quality assertions for Silver layer tables.

Silver tests focus on:
  - Transformation correctness (derived columns, type casts)
  - SCD Type 1: exactly one row per key
  - SCD Type 2: non-overlapping history intervals, open rows have null __END_AT,
    products/customers with CDC events in round 2 have at least 2 history rows
  - Order lifecycle completeness and delivery day plausibility
"""

from conftest import CATALOG, SCHEMA

TABLE = f"{CATALOG}.{SCHEMA}"


def test_silver_order_lifecycle_no_null_order_id(sql):
    rows = sql(
        f"SELECT COUNT(*) AS cnt FROM {TABLE}.silver_order_lifecycle WHERE order_id IS NULL"
    )
    assert int(rows[0]["cnt"]) == 0


def test_silver_order_lifecycle_delivery_days_non_negative(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM {TABLE}.silver_order_lifecycle
        WHERE delivery_days IS NOT NULL AND delivery_days < 0
    """)
    assert int(rows[0]["cnt"]) == 0


def test_silver_order_lifecycle_final_status_valid(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM {TABLE}.silver_order_lifecycle
        WHERE final_status IS NOT NULL
        AND final_status NOT IN ('delivered', 'canceled')
    """)
    assert int(rows[0]["cnt"]) == 0


def test_silver_order_lifecycle_no_duplicate_orders(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM (
            SELECT order_id FROM {TABLE}.silver_order_lifecycle
            GROUP BY order_id HAVING COUNT(*) > 1
        )
    """)
    assert int(rows[0]["cnt"]) == 0


def test_silver_order_items_total_item_value_correct(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM {TABLE}.silver_order_items
        WHERE ABS(total_item_value - ROUND(price + freight_value, 2)) > 0.01
    """)
    assert int(rows[0]["cnt"]) == 0


def test_silver_order_items_valid_price(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM {TABLE}.silver_order_items
        WHERE price <= 0 OR price >= 50000
    """)
    assert int(rows[0]["cnt"]) == 0


def test_silver_order_payments_valid_payment_type(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM {TABLE}.silver_order_payments
        WHERE payment_type NOT IN ('credit_card','boleto','voucher','debit_card')
    """)
    assert int(rows[0]["cnt"]) == 0


def test_silver_order_payments_positive_value(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM {TABLE}.silver_order_payments
        WHERE payment_value <= 0
    """)
    assert int(rows[0]["cnt"]) == 0


def test_silver_order_reviews_sentiment_category_valid(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM {TABLE}.silver_order_reviews
        WHERE sentiment_category NOT IN ('positive', 'neutral', 'negative')
    """)
    assert int(rows[0]["cnt"]) == 0


def test_silver_order_reviews_sentiment_matches_score(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM {TABLE}.silver_order_reviews
        WHERE (review_score >= 4 AND sentiment_category != 'positive')
           OR (review_score = 3  AND sentiment_category != 'neutral')
           OR (review_score <= 2 AND sentiment_category != 'negative')
    """)
    assert int(rows[0]["cnt"]) == 0


def test_silver_products_scd1_one_row_per_product(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM (
            SELECT product_id FROM {TABLE}.silver_products
            GROUP BY product_id HAVING COUNT(*) > 1
        )
    """)
    assert int(rows[0]["cnt"]) == 0


def test_silver_customers_scd1_one_row_per_customer(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM (
            SELECT customer_id FROM {TABLE}.silver_customers
            GROUP BY customer_id HAVING COUNT(*) > 1
        )
    """)
    assert int(rows[0]["cnt"]) == 0


def test_silver_product_price_history_open_rows_have_null_end_at(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM (
            SELECT product_id FROM {TABLE}.silver_product_price_history
            GROUP BY product_id HAVING SUM(CASE WHEN __END_AT IS NULL THEN 1 ELSE 0 END) != 1
        )
    """)
    assert int(rows[0]["cnt"]) == 0, "Every product must have exactly one open SCD2 row"


def test_silver_product_price_history_no_overlapping_intervals(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM (
            SELECT a.product_id
            FROM {TABLE}.silver_product_price_history a
            JOIN {TABLE}.silver_product_price_history b
              ON a.product_id = b.product_id
             AND a.__START_AT < b.__END_AT
             AND b.__START_AT < a.__END_AT
             AND a.__START_AT != b.__START_AT
        )
    """)
    assert int(rows[0]["cnt"]) == 0, "SCD2 intervals must not overlap"


def test_silver_product_price_history_has_multi_row_products(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM (
            SELECT product_id FROM {TABLE}.silver_product_price_history
            GROUP BY product_id HAVING COUNT(*) > 1
        )
    """)
    assert int(rows[0]["cnt"]) > 0, "Round 2 price changes should produce multi-row products"


def test_silver_customer_address_history_open_rows_have_null_end_at(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM (
            SELECT customer_id FROM {TABLE}.silver_customer_address_history
            GROUP BY customer_id HAVING SUM(CASE WHEN __END_AT IS NULL THEN 1 ELSE 0 END) != 1
        )
    """)
    assert int(rows[0]["cnt"]) == 0, "Every customer must have exactly one open SCD2 row"


def test_silver_customer_address_history_has_multi_row_customers(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM (
            SELECT customer_id FROM {TABLE}.silver_customer_address_history
            GROUP BY customer_id HAVING COUNT(*) > 1
        )
    """)
    assert int(rows[0]["cnt"]) > 0, "Round 2 relocations should produce multi-row customers"
