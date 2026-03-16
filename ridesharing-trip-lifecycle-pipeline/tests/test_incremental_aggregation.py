"""
Integration tests for the incremental aggregation path.

These tests validate that:
1. A late-arriving staged file that lands inside the 2-hour listing window is
   picked up on the next incremental run and re-aggregates the affected hour.
2. A late-arriving file that lands in an hour prefix outside the listing window
   is never listed and therefore never reaches aggregation.

moto is used to mock S3 — no real AWS calls are made.
"""
import json
import os
import tempfile
from datetime import datetime, timezone

import boto3
import pytest
from moto import mock_aws

from scripts.glue_trip_aggregator import run_local_staging_aggregation

BUCKET = "test-bucket"
STAGING_PREFIX = "staging/completed_trips"
CHECKPOINT_URI = f"s3://{BUCKET}/checkpoints/test_checkpoint.json"
REGION = "us-east-1"


def make_completed_trip_jsonl(trip_id, pickup_location_id, dropoff_location_id,
                              dropoff_datetime, fare_amount=20.0, tip_amount=2.0,
                              trip_distance=1.5):
    record = {
        "trip_id": trip_id,
        "pickup_location_id": pickup_location_id,
        "dropoff_location_id": dropoff_location_id,
        "dropoff_datetime": dropoff_datetime,
        "fare_amount": fare_amount,
        "tip_amount": tip_amount,
        "trip_distance": trip_distance,
        "trip_status": "completed",
        "data_quality": "ok",
        "record_emitted_at": "2024-05-25T14:06:00+00:00",
        "record_source": "trip_end_update",
    }
    return json.dumps(record)


def put_staging_file(s3_client, key, jsonl_content):
    s3_client.put_object(Bucket=BUCKET, Key=key, Body=jsonl_content.encode("utf-8"))


@mock_aws
def test_incremental_run_picks_up_late_arrival_in_listing_window():
    """
    Scenario:
    - Initial staged file in hour=14 is processed in first run.
    - A late-arriving trip (dropoff_datetime=14:xx) is written to staging/hour=14
      AFTER the first run (simulates Lambda processing the trip late).
    - Second incremental run uses glue_run_time=16:05, so listing window = [hour=14, hour=15].
    - hour=14 is in the window → the new file is picked up → hour=14 is re-aggregated.
    - Summary shows new_objects=1, affected_hours=1, metric_rows>=1.
    """
    s3_client = boto3.client("s3", region_name=REGION)
    s3_client.create_bucket(Bucket=BUCKET)

    # --- First run setup: one trip in hour=14 ---
    initial_key = (
        "staging/completed_trips/year=2024/month=05/day=25/hour=14/"
        "batch_initial.jsonl"
    )
    put_staging_file(
        s3_client,
        initial_key,
        make_completed_trip_jsonl("trip-1", 93, 132, "2024-05-25 14:05:00"),
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        # First run: no checkpoint → full scan → processes hour=14
        summary_1 = run_local_staging_aggregation(
            staging_bucket=BUCKET,
            staging_prefix=STAGING_PREFIX,
            checkpoint_uri=CHECKPOINT_URI,
            output_path=tmpdir,
            region=REGION,
            endpoint_url=None,
        )
        assert summary_1["new_objects"] == 1
        assert summary_1["affected_hours"] == 1

        # --- Late arrival: another trip for hour=14 arrives after first run ---
        late_key = (
            "staging/completed_trips/year=2024/month=05/day=25/hour=14/"
            "batch_late_arrival.jsonl"
        )
        put_staging_file(
            s3_client,
            late_key,
            make_completed_trip_jsonl("trip-2", 93, 132, "2024-05-25 14:42:00"),
        )

        # Second run: glue_run_time=16:05 → listing window = [hour=14, hour=15]
        # hour=14 is in the window and has a new file → re-aggregated
        glue_run_time = datetime(2024, 5, 25, 16, 5)
        summary_2 = run_local_staging_aggregation(
            staging_bucket=BUCKET,
            staging_prefix=STAGING_PREFIX,
            checkpoint_uri=CHECKPOINT_URI,
            output_path=tmpdir,
            region=REGION,
            endpoint_url=None,
            now=glue_run_time,
        )

    assert summary_2["new_objects"] == 1, (
        f"Expected 1 new object (the late arrival), got {summary_2['new_objects']}"
    )
    assert summary_2["affected_hours"] == 1, (
        f"Expected hour=14 to be re-aggregated, got affected_hours={summary_2['affected_hours']}"
    )
    assert summary_2["metric_rows"] >= 1


@mock_aws
def test_incremental_run_skips_hour_outside_listing_window():
    """
    Scenario:
    - Initial staged file in hour=14 is processed in first run.
    - A late-arriving trip (dropoff_datetime=12:xx) is written to staging/hour=12
      well after its hour closed.
    - Second incremental run uses glue_run_time=16:05, so listing window = [hour=14, hour=15].
    - hour=12 is NOT in the listing window → the late file is never listed.
    - Summary shows new_objects=0, nothing re-aggregated.
    """
    s3_client = boto3.client("s3", region_name=REGION)
    s3_client.create_bucket(Bucket=BUCKET)

    # --- First run setup: one trip in hour=14 ---
    initial_key = (
        "staging/completed_trips/year=2024/month=05/day=25/hour=14/"
        "batch_initial.jsonl"
    )
    put_staging_file(
        s3_client,
        initial_key,
        make_completed_trip_jsonl("trip-1", 93, 132, "2024-05-25 14:05:00"),
    )

    with tempfile.TemporaryDirectory() as tmpdir:
        summary_1 = run_local_staging_aggregation(
            staging_bucket=BUCKET,
            staging_prefix=STAGING_PREFIX,
            checkpoint_uri=CHECKPOINT_URI,
            output_path=tmpdir,
            region=REGION,
            endpoint_url=None,
        )
        assert summary_1["new_objects"] == 1

        # --- Very late arrival: trip with dropoff_datetime=12:xx lands in hour=12 ---
        # At glue_run_time=16:05, the listing window is [hour=14, hour=15] — hour=12 is outside.
        very_late_key = (
            "staging/completed_trips/year=2024/month=05/day=25/hour=12/"
            "batch_very_late.jsonl"
        )
        put_staging_file(
            s3_client,
            very_late_key,
            make_completed_trip_jsonl("trip-99", 10, 20, "2024-05-25 12:30:00"),
        )

        # Second run: glue_run_time=16:05 → listing window = [hour=14, hour=15]
        # hour=12 is NOT listed → trip-99 is never seen
        glue_run_time = datetime(2024, 5, 25, 16, 5)
        summary_2 = run_local_staging_aggregation(
            staging_bucket=BUCKET,
            staging_prefix=STAGING_PREFIX,
            checkpoint_uri=CHECKPOINT_URI,
            output_path=tmpdir,
            region=REGION,
            endpoint_url=None,
            now=glue_run_time,
        )

    assert summary_2["new_objects"] == 0, (
        f"Expected 0 new objects (hour=12 outside window), got {summary_2['new_objects']}"
    )
    assert summary_2["affected_hours"] == 0
