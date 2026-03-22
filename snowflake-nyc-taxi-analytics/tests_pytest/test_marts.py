"""
test_marts.py — Integration tests for fact and mart tables.

Tests cover:
  - fct_trips grain uniqueness (trip_id must be unique)
  - mart_revenue_by_zone completeness (every active zone appears)
  - mart_hourly_demand covers all 24 hours
  - Revenue totals agree between fact and mart (within rounding tolerance)
"""

import pytest
from tests_pytest.conftest import query


class TestFctTrips:
    def test_trip_id_uniqueness(self, snowflake_conn, dev_marts_core_schema):
        """
        fct_trips grain is one row per trip.
        Duplicate trip_ids mean the incremental logic has a deduplication bug
        or the surrogate key is insufficiently unique.
        """
        rows = query(
            snowflake_conn,
            f"""
            SELECT COUNT(*) AS cnt FROM (
                SELECT trip_id
                FROM NYC_TAXI.{dev_marts_core_schema}.FCT_TRIPS
                GROUP BY trip_id
                HAVING COUNT(*) > 1
            )
            """
        )
        assert rows[0]["cnt"] == 0, (
            f"Found {rows[0]['cnt']} duplicate trip_id values in fct_trips"
        )

    def test_all_time_of_day_values_valid(self, snowflake_conn, dev_marts_core_schema):
        """
        classify_time_of_day macro must produce only the 5 expected buckets.
        Any other value means the macro has a gap or typo.
        """
        valid_values = {"morning_rush", "midday", "evening_rush", "evening", "overnight"}
        rows = query(
            snowflake_conn,
            f"""
            SELECT DISTINCT time_of_day
            FROM NYC_TAXI.{dev_marts_core_schema}.FCT_TRIPS
            """
        )
        actual_values = {r["time_of_day"] for r in rows}
        unexpected = actual_values - valid_values
        assert not unexpected, (
            f"Unexpected time_of_day values in fct_trips: {unexpected}"
        )

    def test_no_null_pickup_locations(self, snowflake_conn, dev_marts_core_schema):
        rows = query(
            snowflake_conn,
            f"""
            SELECT COUNT(*) AS cnt
            FROM NYC_TAXI.{dev_marts_core_schema}.FCT_TRIPS
            WHERE pickup_location_id IS NULL
            """
        )
        assert rows[0]["cnt"] == 0, "Found NULL pickup_location_id in fct_trips"


class TestMartRevenueByZone:
    def test_every_active_zone_represented(
        self, snowflake_conn, dev_marts_core_schema, dev_marts_finance_schema
    ):
        """
        Every zone that has at least one trip in fct_trips must appear in
        mart_revenue_by_zone.  Missing zones indicate a JOIN or GROUP BY bug.
        """
        rows = query(
            snowflake_conn,
            f"""
            SELECT COUNT(*) AS cnt FROM (
                SELECT DISTINCT pickup_location_id
                FROM NYC_TAXI.{dev_marts_core_schema}.FCT_TRIPS
                WHERE pickup_location_id IS NOT NULL
            ) trips
            LEFT JOIN (
                SELECT DISTINCT pickup_location_id
                FROM NYC_TAXI.{dev_marts_finance_schema}.MART_REVENUE_BY_ZONE
            ) mart USING (pickup_location_id)
            WHERE mart.pickup_location_id IS NULL
            """
        )
        assert rows[0]["cnt"] == 0, (
            f"{rows[0]['cnt']} zones in fct_trips are missing from mart_revenue_by_zone"
        )

    def test_total_revenue_agrees_with_fact(
        self, snowflake_conn, dev_marts_core_schema, dev_marts_finance_schema
    ):
        """
        Sum of total_revenue in mart_revenue_by_zone should equal sum of
        total_amount in fct_trips (within $1 floating-point rounding tolerance).
        """
        fact_total = query(
            snowflake_conn,
            f"SELECT SUM(total_amount) AS total FROM NYC_TAXI.{dev_marts_core_schema}.FCT_TRIPS"
        )[0]["total"]

        mart_total = query(
            snowflake_conn,
            f"""
            SELECT SUM(total_revenue) AS total
            FROM NYC_TAXI.{dev_marts_finance_schema}.MART_REVENUE_BY_ZONE
            """
        )[0]["total"]

        assert fact_total is not None, "fct_trips.total_amount sum is NULL"
        assert mart_total is not None, "mart_revenue_by_zone.total_revenue sum is NULL"
        assert abs(fact_total - mart_total) < 1.0, (
            f"Revenue totals disagree: fct_trips={fact_total:.2f}, "
            f"mart_revenue_by_zone={mart_total:.2f} (diff > $1)"
        )

    def test_trip_counts_are_positive(self, snowflake_conn, dev_marts_finance_schema):
        rows = query(
            snowflake_conn,
            f"""
            SELECT COUNT(*) AS cnt
            FROM NYC_TAXI.{dev_marts_finance_schema}.MART_REVENUE_BY_ZONE
            WHERE trip_count <= 0
            """
        )
        assert rows[0]["cnt"] == 0, "Found zero or negative trip_count in mart_revenue_by_zone"


class TestMartHourlyDemand:
    def test_covers_all_24_hours(self, snowflake_conn, dev_marts_finance_schema):
        """
        The mart must contain exactly 24 distinct hour values (0 through 23).
        A missing hour means trips in that hour were dropped by the GROUP BY
        or there is genuinely no data — but with 9.5M rows, every hour will have trips.
        """
        rows = query(
            snowflake_conn,
            f"""
            SELECT COUNT(DISTINCT pickup_hour) AS cnt
            FROM NYC_TAXI.{dev_marts_finance_schema}.MART_HOURLY_DEMAND
            """
        )
        assert rows[0]["cnt"] == 24, (
            f"mart_hourly_demand has {rows[0]['cnt']} distinct hours, expected 24"
        )

    def test_covers_all_7_weekdays(self, snowflake_conn, dev_marts_finance_schema):
        rows = query(
            snowflake_conn,
            f"""
            SELECT COUNT(DISTINCT pickup_day_of_week) AS cnt
            FROM NYC_TAXI.{dev_marts_finance_schema}.MART_HOURLY_DEMAND
            """
        )
        assert rows[0]["cnt"] == 7, (
            f"mart_hourly_demand has {rows[0]['cnt']} distinct weekdays, expected 7"
        )

    def test_all_time_of_day_buckets_present(self, snowflake_conn, dev_marts_finance_schema):
        expected = {"morning_rush", "midday", "evening_rush", "evening", "overnight"}
        rows = query(
            snowflake_conn,
            f"""
            SELECT DISTINCT time_of_day
            FROM NYC_TAXI.{dev_marts_finance_schema}.MART_HOURLY_DEMAND
            """
        )
        actual = {r["time_of_day"] for r in rows}
        assert actual == expected, (
            f"Expected time_of_day buckets {expected}, found {actual}"
        )
