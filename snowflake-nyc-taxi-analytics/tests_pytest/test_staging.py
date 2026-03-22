"""
test_staging.py — Integration tests for the stg_yellow_trips staging view.

These tests query the DEV_STAGING.STG_YELLOW_TRIPS view directly in Snowflake
to verify:
  - Row count parity between raw source and staging view
  - No null trip IDs (surrogate key integrity)
  - No trips with zero or negative duration after staging filters
  - Column types are correct (using information_schema introspection)

"""

import pytest
from tests_pytest.conftest import query


class TestStagingRowCounts:
    def test_row_count_matches_source(self, snowflake_conn, dev_staging_schema):
        raw_count = query(
            snowflake_conn,
            "SELECT COUNT(*) AS cnt FROM NYC_TAXI.RAW.YELLOW_TRIPDATA"
        )[0]["cnt"]

        staging_count = query(
            snowflake_conn,
            f"SELECT COUNT(*) AS cnt FROM NYC_TAXI.{dev_staging_schema}.STG_YELLOW_TRIPS"
        )[0]["cnt"]

        assert staging_count > 0, "Staging view is empty — has dbt run been executed?"
        assert staging_count <= raw_count, (
            f"Staging ({staging_count}) should not exceed raw ({raw_count})"
        )
        # Allow up to 0.1% row loss from filtering — more than that indicates a bug
        loss_pct = (raw_count - staging_count) / raw_count
        assert loss_pct < 0.001, (
            f"Too many rows filtered: {raw_count - staging_count:,} lost "
            f"({loss_pct:.4%} of raw rows)"
        )


class TestStagingKeyIntegrity:
    def test_no_null_trip_ids(self, snowflake_conn, dev_staging_schema):
        """trip_id is the surrogate key — no NULLs allowed."""
        rows = query(
            snowflake_conn,
            f"""
            SELECT COUNT(*) AS cnt
            FROM NYC_TAXI.{dev_staging_schema}.STG_YELLOW_TRIPS
            WHERE trip_id IS NULL
            """
        )
        assert rows[0]["cnt"] == 0, "Found rows with NULL trip_id in stg_yellow_trips"

    def test_trip_id_uniqueness(self, snowflake_conn, dev_staging_schema):
        """trip_id must be unique — duplicates indicate a surrogate key collision."""
        rows = query(
            snowflake_conn,
            f"""
            SELECT COUNT(*) AS cnt FROM (
                SELECT trip_id
                FROM NYC_TAXI.{dev_staging_schema}.STG_YELLOW_TRIPS
                GROUP BY trip_id
                HAVING COUNT(*) > 1
            )
            """
        )
        assert rows[0]["cnt"] == 0, (
            f"Found {rows[0]['cnt']} duplicate trip_id values in stg_yellow_trips"
        )


class TestStagingDataQuality:
    def test_trip_duration_positive(self, snowflake_conn, dev_staging_schema):
        """
        Staging filters out rows where dropoff <= pickup.
        No row should have trip_duration_minutes <= 0.
        """
        rows = query(
            snowflake_conn,
            f"""
            SELECT COUNT(*) AS cnt
            FROM NYC_TAXI.{dev_staging_schema}.STG_YELLOW_TRIPS
            WHERE trip_duration_minutes <= 0
            """
        )
        assert rows[0]["cnt"] == 0, (
            "Found trips with non-positive duration — staging filter may be broken"
        )

    def test_pickup_datetime_not_null(self, snowflake_conn, dev_staging_schema):
        rows = query(
            snowflake_conn,
            f"""
            SELECT COUNT(*) AS cnt
            FROM NYC_TAXI.{dev_staging_schema}.STG_YELLOW_TRIPS
            WHERE pickup_datetime IS NULL
            """
        )
        assert rows[0]["cnt"] == 0, "Found NULL pickup_datetime in stg_yellow_trips"


class TestStagingColumnTypes:
    def test_pickup_datetime_is_timestamp(self, snowflake_conn, dev_staging_schema):
        """
        Introspect information_schema to verify pickup_datetime was cast correctly.
        This catches regressions where a schema change in the raw table causes
        the CAST to silently produce a VARCHAR instead of TIMESTAMP_NTZ.
        """
        rows = query(
            snowflake_conn,
            f"""
            SELECT data_type
            FROM NYC_TAXI.INFORMATION_SCHEMA.COLUMNS
            WHERE table_schema = '{dev_staging_schema}'
              AND table_name   = 'STG_YELLOW_TRIPS'
              AND column_name  = 'PICKUP_DATETIME'
            """
        )
        assert len(
            rows) == 1, "Column PICKUP_DATETIME not found in information_schema"
        assert "TIMESTAMP" in rows[0]["data_type"].upper(), (
            f"Expected PICKUP_DATETIME to be TIMESTAMP type, got: {rows[0]['data_type']}"
        )

    def test_trip_id_is_varchar(self, snowflake_conn, dev_staging_schema):
        """trip_id is an MD5 hash — must be VARCHAR, not a numeric type."""
        rows = query(
            snowflake_conn,
            f"""
            SELECT data_type
            FROM NYC_TAXI.INFORMATION_SCHEMA.COLUMNS
            WHERE table_schema = '{dev_staging_schema}'
              AND table_name   = 'STG_YELLOW_TRIPS'
              AND column_name  = 'TRIP_ID'
            """
        )
        assert len(rows) == 1, "Column TRIP_ID not found in information_schema"
        assert rows[0]["data_type"].upper() in ("TEXT", "VARCHAR"), (
            f"Expected TRIP_ID to be TEXT/VARCHAR, got: {rows[0]['data_type']}"
        )
