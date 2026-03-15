from datetime import datetime, timezone

from pyspark.sql import SparkSession

from scripts.glue_trip_aggregator import (
    aggregate_completed_trips,
    build_checkpoint_state,
    collect_affected_partition_prefixes,
    resolve_runtime_option,
    select_new_stage_objects,
    to_int_safe,
    transform_top_routes_per_hour,
)


def staged_row(
    trip_id,
    pickup_location_id,
    dropoff_location_id,
    dropoff_datetime,
    fare_amount,
    tip_amount,
    trip_distance,
    record_emitted_at,
):
    return {
        "trip_id": trip_id,
        "pickup_location_id": pickup_location_id,
        "dropoff_location_id": dropoff_location_id,
        "dropoff_datetime": dropoff_datetime,
        "fare_amount": fare_amount,
        "tip_amount": tip_amount,
        "trip_distance": trip_distance,
        "trip_status": "completed",
        "data_quality": "ok",
        "record_emitted_at": record_emitted_at,
        "record_source": "trip_end_update",
    }


def test_aggregate_completed_trips_deduplicates_and_groups_hourly():
    rows = [
        staged_row("trip-1", 93, 132, "2024-05-25 14:05:00", 40.0, 2.0, 1.5, "2024-05-25T14:06:00+00:00"),
        staged_row("trip-1", 93, 132, "2024-05-25 14:05:00", 45.0, 3.0, 1.7, "2024-05-25T14:07:00+00:00"),
        staged_row("trip-2", 93, 132, "2024-05-25 14:42:00", 20.5, 1.0, 1.0, "2024-05-25T14:43:00+00:00"),
        {
            "trip_id": "trip-3",
            "pickup_location_id": 93,
            "dropoff_location_id": 132,
            "dropoff_datetime": "2024-05-25 14:15:00",
            "fare_amount": 100.0,
            "tip_amount": 10.0,
            "trip_distance": 8.0,
            "trip_status": "completed",
            "data_quality": "missing_start",
            "record_emitted_at": "2024-05-25T14:16:00+00:00",
            "record_source": "trip_end_upsert",
        },
    ]

    result = aggregate_completed_trips(rows)

    assert len(result) == 1
    record = result[0]
    assert record["event_hour"] == "2024-05-25 14:00:00"
    assert record["trip_count"] == 2
    assert record["fare_sum"] == 65.5
    assert record["tip_sum"] == 4.0
    assert record["distance_sum"] == 2.7
    assert record["year"] == "2024"
    assert record["month"] == "05"
    assert record["day"] == "25"
    assert record["hour"] == "14"


def test_transform_top_routes_per_hour_ranks_routes_with_window_function():
    rows = [
        staged_row("trip-1", 10, 20, "2024-05-25 14:05:00", 30.0, 3.0, 2.0, "2024-05-25T14:06:00+00:00"),
        staged_row("trip-2", 10, 20, "2024-05-25 14:15:00", 20.0, 2.0, 1.0, "2024-05-25T14:16:00+00:00"),
        staged_row("trip-3", 11, 21, "2024-05-25 14:20:00", 100.0, 5.0, 5.0, "2024-05-25T14:21:00+00:00"),
        staged_row("trip-4", 12, 22, "2024-05-25 15:10:00", 60.0, 6.0, 3.0, "2024-05-25T15:11:00+00:00"),
        staged_row("trip-5", 13, 23, "2024-05-25 15:30:00", 10.0, 1.0, 1.0, "2024-05-25T15:31:00+00:00"),
    ]

    spark = SparkSession.builder.master("local[1]").appName("test-top-routes").getOrCreate()
    try:
        source_frame = spark.createDataFrame(rows)
        output_rows = (
            transform_top_routes_per_hour(source_frame, top_limit=1)
            .select(
                "event_hour",
                "pickup_location_id",
                "dropoff_location_id",
                "trip_count",
                "route_rank_in_hour",
                "avg_fare_per_trip",
                "tip_to_fare_ratio",
            )
            .orderBy("event_hour")
            .collect()
        )
    finally:
        spark.stop()

    assert len(output_rows) == 2

    first_hour = output_rows[0].asDict()
    assert str(first_hour["event_hour"]) == "2024-05-25 14:00:00"
    assert first_hour["pickup_location_id"] == 10
    assert first_hour["dropoff_location_id"] == 20
    assert first_hour["trip_count"] == 2
    assert first_hour["route_rank_in_hour"] == 1
    assert first_hour["avg_fare_per_trip"] == 25.0
    assert first_hour["tip_to_fare_ratio"] == 0.1

    second_hour = output_rows[1].asDict()
    assert str(second_hour["event_hour"]) == "2024-05-25 15:00:00"
    assert second_hour["pickup_location_id"] == 12
    assert second_hour["dropoff_location_id"] == 22
    assert second_hour["trip_count"] == 1
    assert second_hour["route_rank_in_hour"] == 1


def test_select_new_stage_objects_uses_checkpoint_marker():
    object_rows = [
        {
            "Key": "staging/completed_trips/year=2024/month=05/day=25/hour=13/file-a.jsonl",
            "LastModified": datetime(2024, 5, 25, 13, 30, tzinfo=timezone.utc),
        },
        {
            "Key": "staging/completed_trips/year=2024/month=05/day=25/hour=14/file-b.jsonl",
            "LastModified": datetime(2024, 5, 25, 14, 30, tzinfo=timezone.utc),
        },
        {
            "Key": "staging/completed_trips/year=2024/month=05/day=25/hour=14/file-c.jsonl",
            "LastModified": datetime(2024, 5, 25, 14, 30, tzinfo=timezone.utc),
        },
    ]
    checkpoint_state = {
        "last_modified": "2024-05-25T14:30:00+00:00",
        "last_key": "staging/completed_trips/year=2024/month=05/day=25/hour=14/file-b.jsonl",
    }

    new_rows = select_new_stage_objects(object_rows, checkpoint_state)

    assert len(new_rows) == 1
    assert new_rows[0]["Key"].endswith("file-c.jsonl")


def test_collect_affected_partition_prefixes_and_checkpoint_state():
    object_rows = [
        {
            "Key": "staging/completed_trips/year=2024/month=05/day=25/hour=14/file-b.jsonl",
            "LastModified": datetime(2024, 5, 25, 14, 30, tzinfo=timezone.utc),
        },
        {
            "Key": "staging/completed_trips/year=2024/month=05/day=25/hour=15/file-c.jsonl",
            "LastModified": datetime(2024, 5, 25, 15, 1, tzinfo=timezone.utc),
        },
    ]

    prefixes = collect_affected_partition_prefixes(object_rows)
    checkpoint_state = build_checkpoint_state(object_rows)

    assert prefixes == [
        "staging/completed_trips/year=2024/month=05/day=25/hour=14",
        "staging/completed_trips/year=2024/month=05/day=25/hour=15",
    ]
    assert checkpoint_state["last_key"].endswith("file-c.jsonl")
    assert checkpoint_state["processed_object_count"] == 2


def test_resolve_runtime_option_supports_underscore_and_dash():
    argv = [
        "job.py",
        "--top_routes_output_path",
        "s3://bucket/path-a",
        "--top-routes-limit=7",
    ]

    assert resolve_runtime_option(argv, "top_routes_output_path") == "s3://bucket/path-a"
    assert resolve_runtime_option(argv, "top_routes_limit") == "7"
    assert resolve_runtime_option(argv, "missing_key", "fallback") == "fallback"


def test_to_int_safe_falls_back_to_default():
    assert to_int_safe("9", 3) == 9
    assert to_int_safe("invalid", 3) == 3
    assert to_int_safe(None, 5) == 5
