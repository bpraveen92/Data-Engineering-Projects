"""
conftest.py — shared pytest fixtures for DLT pipeline data quality tests.

Connects to the Databricks SQL warehouse via the Databricks SDK and provides
a reusable sql() helper that executes a query and returns rows as a list of
dicts. Tests run against the live ecommerce_dlt.ecommerce_dev schema.

Warehouse ID is read from the DATABRICKS_WAREHOUSE_ID environment variable,
falling back to the hardcoded dev warehouse. Host and token are read from
the default Databricks CLI profile (~/.databrickscfg).
"""

import os

import pytest
from databricks.sdk import WorkspaceClient
from databricks.sdk.service.sql import StatementState


CATALOG = "ecommerce_dlt"
SCHEMA = "ecommerce_dev"
WAREHOUSE_ID = os.getenv("DATABRICKS_WAREHOUSE_ID", "9f2ff24853ee869f")


@pytest.fixture(scope="session")
def client():
    return WorkspaceClient()


@pytest.fixture(scope="session")
def sql(client):
    def run(query):
        import time

        response = client.statement_execution.execute_statement(
            warehouse_id=WAREHOUSE_ID,
            statement=query,
            wait_timeout="50s",
        )
        # Poll until terminal state for queries that exceed the wait timeout
        while response.status.state in (
            StatementState.PENDING,
            StatementState.RUNNING,
        ):
            time.sleep(2)
            response = client.statement_execution.get_statement(
                response.statement_id
            )
        if response.status.state != StatementState.SUCCEEDED:
            raise RuntimeError(
                f"Query failed ({response.status.state}): {response.status.error}"
            )
        result = response.result
        if not result or not result.data_array:
            return []
        columns = [col.name for col in response.manifest.schema.columns]
        return [dict(zip(columns, row)) for row in result.data_array]

    return run
