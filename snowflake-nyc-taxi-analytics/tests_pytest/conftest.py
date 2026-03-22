"""
conftest.py — Shared pytest fixtures for Snowflake integration tests.

These tests run AFTER `make dbt-run` has populated the dev schemas.
They connect directly to Snowflake and query the materialized tables,
so they test the actual output.
"""

import os
import pytest
from utils.snowflake_loader import get_connection


@pytest.fixture(scope="session")
def snowflake_conn():
    """
    Open a Snowflake connection to the dev database for the test session.
    Yields the connection so it is properly closed even if tests fail.
    """
    target = os.environ.get("DBT_TARGET", "dev")
    database = "NYC_TAXI" if target == "dev" else "NYC_TAXI_PROD"

    conn = get_connection(database=database, schema="RAW")
    yield conn
    conn.close()


@pytest.fixture(scope="session")
def dev_staging_schema():
    target = os.environ.get("DBT_TARGET", "dev")
    return "DEV_STAGING" if target == "dev" else "STAGING"


@pytest.fixture(scope="session")
def dev_marts_core_schema():
    target = os.environ.get("DBT_TARGET", "dev")
    return "DEV_MARTS_CORE" if target == "dev" else "MARTS_CORE"


@pytest.fixture(scope="session")
def dev_marts_finance_schema():
    target = os.environ.get("DBT_TARGET", "dev")
    return "DEV_MARTS_FINANCE" if target == "dev" else "MARTS_FINANCE"


def query(conn, sql):
    """Execute SQL and return results as a list of dicts (column_name → value)."""
    with conn.cursor() as cur:
        cur.execute(sql)
        columns = [col[0].lower() for col in cur.description]
        return [dict(zip(columns, row)) for row in cur.fetchall()]
