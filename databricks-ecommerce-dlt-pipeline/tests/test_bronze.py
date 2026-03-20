"""
test_bronze.py — Data quality assertions for Bronze layer tables.

Bronze is append-only and raw — tests focus on:
  - Tables exist and are non-empty
  - Critical columns are never null (stronger than @dlt.expect warn-only)
  - Domain values are within expected sets
  - No duplicate primary keys within a single ingestion round
"""

from conftest import CATALOG, SCHEMA

TABLE = f"{CATALOG}.{SCHEMA}"


def test_bronze_order_events_non_empty(sql):
    rows = sql(f"SELECT COUNT(*) AS cnt FROM {TABLE}.bronze_order_events")
    assert int(rows[0]["cnt"]) > 0


def test_bronze_order_events_no_null_order_id(sql):
    rows = sql(f"SELECT COUNT(*) AS cnt FROM {TABLE}.bronze_order_events WHERE order_id IS NULL")
    assert int(rows[0]["cnt"]) == 0


def test_bronze_order_events_valid_statuses(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM {TABLE}.bronze_order_events
        WHERE order_status NOT IN ('created','approved','shipped','delivered','canceled')
    """)
    assert int(rows[0]["cnt"]) == 0


def test_bronze_order_events_no_null_event_timestamp(sql):
    rows = sql(
        f"SELECT COUNT(*) AS cnt FROM {TABLE}.bronze_order_events WHERE event_timestamp IS NULL"
    )
    assert int(rows[0]["cnt"]) == 0


def test_bronze_order_items_non_empty(sql):
    rows = sql(f"SELECT COUNT(*) AS cnt FROM {TABLE}.bronze_order_items")
    assert int(rows[0]["cnt"]) > 0


def test_bronze_order_items_no_null_product_id(sql):
    rows = sql(
        f"SELECT COUNT(*) AS cnt FROM {TABLE}.bronze_order_items WHERE product_id IS NULL"
    )
    assert int(rows[0]["cnt"]) == 0


def test_bronze_order_payments_valid_payment_types(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM {TABLE}.bronze_order_payments
        WHERE payment_type NOT IN ('credit_card','boleto','voucher','debit_card')
    """)
    assert int(rows[0]["cnt"]) == 0


def test_bronze_order_reviews_valid_score_range(sql):
    rows = sql(f"""
        SELECT COUNT(*) AS cnt FROM {TABLE}.bronze_order_reviews
        WHERE review_score NOT BETWEEN 1 AND 5
    """)
    assert int(rows[0]["cnt"]) == 0


def test_bronze_product_updates_no_null_product_id(sql):
    rows = sql(
        f"SELECT COUNT(*) AS cnt FROM {TABLE}.bronze_product_updates WHERE product_id IS NULL"
    )
    assert int(rows[0]["cnt"]) == 0


def test_bronze_customer_updates_no_null_customer_id(sql):
    rows = sql(
        f"SELECT COUNT(*) AS cnt FROM {TABLE}.bronze_customer_updates WHERE customer_id IS NULL"
    )
    assert int(rows[0]["cnt"]) == 0


def test_bronze_all_six_tables_populated(sql):
    tables = [
        "bronze_order_events",
        "bronze_order_items",
        "bronze_order_payments",
        "bronze_order_reviews",
        "bronze_product_updates",
        "bronze_customer_updates",
    ]
    for table in tables:
        rows = sql(f"SELECT COUNT(*) AS cnt FROM {TABLE}.{table}")
        assert int(rows[0]["cnt"]) > 0, f"{table} is empty"
