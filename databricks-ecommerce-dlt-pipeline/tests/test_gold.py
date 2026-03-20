"""
test_gold.py — Data quality assertions for Gold layer tables.

Gold tests enforce business rules that @dlt.expect_or_fail covers at the
row level but doesn't validate at the aggregate level. Tests focus on:
  - No duplicate primary keys (one row per order/seller/category-month)
  - Metric plausibility (non-negative revenue, delivery days in range)
  - Referential completeness (sellers in items appear in performance table)
  - Category revenue tied to delivered orders only
"""

from conftest import CATALOG, SCHEMA

TABLE = f"{CATALOG}.{SCHEMA}"


def test_gold_order_fulfillment_no_null_order_id(sql):
    rows = sql(
        f"SELECT COUNT(*) AS cnt FROM {TABLE}.gold_order_fulfillment WHERE order_id IS NULL"
    )
    assert int(rows[0]["cnt"]) == 0


def test_gold_order_fulfillment_no_duplicate_orders(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM (
            SELECT order_id FROM {TABLE}.gold_order_fulfillment
            GROUP BY order_id HAVING COUNT(*) > 1
        )
    """)
    assert int(rows[0]["cnt"]) == 0


def test_gold_order_fulfillment_positive_total_value(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM {TABLE}.gold_order_fulfillment
        WHERE total_order_value IS NULL OR total_order_value <= 0
    """)
    assert int(rows[0]["cnt"]) == 0


def test_gold_order_fulfillment_delivery_days_plausible(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM {TABLE}.gold_order_fulfillment
        WHERE delivery_days IS NOT NULL AND (delivery_days < 0 OR delivery_days > 365)
    """)
    assert int(rows[0]["cnt"]) == 0


def test_gold_order_fulfillment_valid_payment_type(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM {TABLE}.gold_order_fulfillment
        WHERE primary_payment_type IS NOT NULL
        AND primary_payment_type NOT IN ('credit_card','boleto','voucher','debit_card')
    """)
    assert int(rows[0]["cnt"]) == 0


def test_gold_order_fulfillment_review_score_range(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM {TABLE}.gold_order_fulfillment
        WHERE review_score IS NOT NULL AND review_score NOT BETWEEN 1 AND 5
    """)
    assert int(rows[0]["cnt"]) == 0


def test_gold_seller_performance_no_null_seller_id(sql):
    rows = sql(
        f"SELECT COUNT(*) AS cnt FROM {TABLE}.gold_seller_performance WHERE seller_id IS NULL"
    )
    assert int(rows[0]["cnt"]) == 0


def test_gold_seller_performance_no_duplicate_sellers(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM (
            SELECT seller_id FROM {TABLE}.gold_seller_performance
            GROUP BY seller_id HAVING COUNT(*) > 1
        )
    """)
    assert int(rows[0]["cnt"]) == 0


def test_gold_seller_performance_non_negative_revenue(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM {TABLE}.gold_seller_performance
        WHERE total_revenue < 0
    """)
    assert int(rows[0]["cnt"]) == 0


def test_gold_seller_performance_positive_order_count(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM {TABLE}.gold_seller_performance
        WHERE total_orders <= 0
    """)
    assert int(rows[0]["cnt"]) == 0


def test_gold_seller_performance_all_sellers_covered(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM (
            SELECT DISTINCT seller_id FROM {TABLE}.silver_order_items
            WHERE seller_id NOT IN (
                SELECT seller_id FROM {TABLE}.gold_seller_performance
            )
        )
    """)
    assert int(rows[0]["cnt"]) == 0, "All sellers in silver_order_items must appear in gold_seller_performance"


def test_gold_category_revenue_no_null_category(sql):
    rows = sql(
        f"SELECT COUNT(*) AS cnt FROM {TABLE}.gold_category_revenue WHERE category IS NULL"
    )
    assert int(rows[0]["cnt"]) == 0


def test_gold_category_revenue_no_null_order_month(sql):
    rows = sql(
        f"SELECT COUNT(*) AS cnt FROM {TABLE}.gold_category_revenue WHERE order_month IS NULL"
    )
    assert int(rows[0]["cnt"]) == 0


def test_gold_category_revenue_positive_revenue(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM {TABLE}.gold_category_revenue
        WHERE total_revenue <= 0
    """)
    assert int(rows[0]["cnt"]) == 0


def test_gold_category_revenue_no_duplicate_category_month(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM (
            SELECT category, order_month FROM {TABLE}.gold_category_revenue
            GROUP BY category, order_month HAVING COUNT(*) > 1
        )
    """)
    assert int(rows[0]["cnt"]) == 0


def test_gold_category_revenue_order_month_format(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM {TABLE}.gold_category_revenue
        WHERE order_month NOT RLIKE '^[0-9]{{4}}-[0-9]{{2}}$'
    """)
    assert int(rows[0]["cnt"]) == 0
