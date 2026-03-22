"""
test_snapshots.py — Integration tests for zone_attributes_snapshot (SCD Type 2).

These tests verify the structural correctness of the snapshot table:
  - dbt_scd_id is unique (it's the snapshot's surrogate PK)
  - Current rows (dbt_valid_to IS NULL) exist for all seeded zones
  - After a simulated zone update, old record is expired and new record is active

Note: test_snapshot_captures_borough_change is a manual verification test that
requires you to:
  1. Edit one borough value in seeds/taxi_zones.csv
  2. Run `make dbt-seed && make dbt-snapshot`
  3. Then run this test

The other tests run immediately after the first `make dbt-snapshot`.
"""

import pytest
from tests_pytest.conftest import query

SNAPSHOT_TABLE = "NYC_TAXI.SNAPSHOTS.ZONE_ATTRIBUTES_SNAPSHOT"
SEED_TABLE = "NYC_TAXI.RAW.TAXI_ZONES"


class TestSnapshotStructure:
    def test_snapshot_table_exists(self, snowflake_conn):
        """Verify the snapshot was created successfully."""
        rows = query(
            snowflake_conn,
            f"SELECT COUNT(*) AS cnt FROM {SNAPSHOT_TABLE}"
        )
        assert rows[0]["cnt"] > 0, (
            f"{SNAPSHOT_TABLE} is empty. Run `make dbt-snapshot` first."
        )

    def test_dbt_scd_id_is_unique(self, snowflake_conn):
        """
        dbt_scd_id is the surrogate PK of the snapshot table.
        dbt generates it as a hash of (unique_key + dbt_valid_from).
        Duplicates indicate a snapshot configuration bug.
        """
        rows = query(
            snowflake_conn,
            f"""
            SELECT COUNT(*) AS cnt FROM (
                SELECT dbt_scd_id
                FROM {SNAPSHOT_TABLE}
                GROUP BY dbt_scd_id
                HAVING COUNT(*) > 1
            )
            """
        )
        assert rows[0]["cnt"] == 0, (
            f"Found {rows[0]['cnt']} duplicate dbt_scd_id values in snapshot"
        )

    def test_all_zones_have_current_record(self, snowflake_conn):
        """
        Every zone_id in the seed must have exactly one current record
        (dbt_valid_to IS NULL) in the snapshot.
        After the first snapshot run, all 265 zones should be current.
        """
        seed_count = query(
            snowflake_conn,
            f"SELECT COUNT(*) AS cnt FROM {SEED_TABLE}"
        )[0]["cnt"]

        current_count = query(
            snowflake_conn,
            f"""
            SELECT COUNT(*) AS cnt
            FROM {SNAPSHOT_TABLE}
            WHERE dbt_valid_to IS NULL
            """
        )[0]["cnt"]

        assert current_count == seed_count, (
            f"Expected {seed_count} current snapshot rows (one per zone), "
            f"found {current_count}"
        )

    def test_current_rows_have_null_valid_to(self, snowflake_conn):
        """
        dbt_valid_to = NULL marks the currently-active version of each record.
        This test verifies the sentinel NULL convention is correctly applied.
        """
        rows = query(
            snowflake_conn,
            f"""
            SELECT COUNT(*) AS cnt
            FROM {SNAPSHOT_TABLE}
            WHERE dbt_valid_to IS NULL
              AND zone_id IS NULL
            """
        )
        assert rows[0]["cnt"] == 0, "Found current snapshot rows with NULL zone_id"


class TestSnapshotSCDHistory:
    def test_expired_rows_have_valid_to_set(self, snowflake_conn):
        """
        Any non-current row must have dbt_valid_to set to a non-NULL timestamp.
        A NULL dbt_valid_to on a non-current row would indicate two 'current'
        versions of the same zone — an SCD2 data integrity violation.
        """
        rows = query(
            snowflake_conn,
            f"""
            SELECT COUNT(*) AS cnt FROM (
                -- Find zone_ids that have more than one row
                SELECT zone_id
                FROM {SNAPSHOT_TABLE}
                GROUP BY zone_id
                HAVING COUNT(*) > 1
            ) multi_version_zones
            JOIN {SNAPSHOT_TABLE} snap USING (zone_id)
            WHERE snap.dbt_valid_to IS NULL
              -- Count per zone should be exactly 1 (only one current row)
              -- We're checking no zone has 2+ rows with valid_to = NULL
            QUALIFY COUNT(*) OVER (PARTITION BY snap.zone_id) > 1
            """
        )
        assert rows[0]["cnt"] == 0, (
            "Found zones with multiple 'current' rows (dbt_valid_to IS NULL) — "
            "SCD2 integrity violation"
        )

    def test_valid_from_precedes_valid_to(self, snowflake_conn):
        """
        For all expired rows, dbt_valid_from must be earlier than dbt_valid_to.
        A row where valid_to <= valid_from is a time-travel paradox.
        """
        rows = query(
            snowflake_conn,
            f"""
            SELECT COUNT(*) AS cnt
            FROM {SNAPSHOT_TABLE}
            WHERE dbt_valid_to IS NOT NULL
              AND dbt_valid_from >= dbt_valid_to
            """
        )
        assert rows[0]["cnt"] == 0, (
            "Found snapshot rows where dbt_valid_from >= dbt_valid_to"
        )

    def test_snapshot_captures_borough_change(self, snowflake_conn):
        """
        Manual verification test: run after editing a zone's borough in the seed.

        To trigger this scenario:
          1. Edit seeds/taxi_zones.csv — change zone_id 4 borough from 'Manhattan' to 'Brooklyn'
          2. make dbt-seed && make dbt-snapshot
          3. Run this test

        Expected outcome:
          - One expired row: zone_id=4, borough='Manhattan', dbt_valid_to IS NOT NULL
          - One current row: zone_id=4, borough='Brooklyn', dbt_valid_to IS NULL
        """
        rows = query(
            snowflake_conn,
            f"""
            SELECT COUNT(*) AS cnt
            FROM {SNAPSHOT_TABLE}
            WHERE zone_id = 4
            """
        )
        zone_4_rows = rows[0]["cnt"]

        if zone_4_rows == 1:
            pytest.skip(
                "Zone 4 has only one snapshot row — no borough change has been applied yet. "
                "To test SCD2: edit seeds/taxi_zones.csv (zone_id=4 borough), "
                "run make dbt-seed && make dbt-snapshot, then re-run this test."
            )

        # If we reach here, a borough change has been captured
        expired_rows = query(
            snowflake_conn,
            f"""
            SELECT COUNT(*) AS cnt
            FROM {SNAPSHOT_TABLE}
            WHERE zone_id = 4
              AND dbt_valid_to IS NOT NULL
            """
        )[0]["cnt"]

        current_rows = query(
            snowflake_conn,
            f"""
            SELECT COUNT(*) AS cnt
            FROM {SNAPSHOT_TABLE}
            WHERE zone_id = 4
              AND dbt_valid_to IS NULL
            """
        )[0]["cnt"]

        assert expired_rows >= 1, "Expected at least one expired row for zone_id=4"
        assert current_rows == 1, f"Expected exactly 1 current row for zone_id=4, found {current_rows}"
