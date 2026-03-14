from pyspark.sql import SparkSession

from scripts.glue_trip_aggregator import (
    aggregate_completed_trips,
    resolve_runtime_option,
    to_int_safe,
    transform_top_routes_per_hour,
)


def test_aggregate_completed_trips_filters_and_groups_hourly():
    rows = [
        {
            "trip_status": "completed",
            "data_quality": "ok",
            "start_pickup_location_id": 93,
            "start_dropoff_location_id": 93,
            "end_dropoff_datetime": "2024-05-25 14:05:00",
            "end_fare_amount": 40.0,
            "end_tip_amount": 2.0,
            "end_trip_distance": 1.5,
        },
        {
            "trip_status": "completed",
            "data_quality": "ok",
            "start_pickup_location_id": 93,
            "start_dropoff_location_id": 93,
            "end_dropoff_datetime": "2024-05-25 14:42:00",
            "end_fare_amount": 20.5,
            "end_tip_amount": 1.0,
            "end_trip_distance": 1.0,
        },
        {
            "trip_status": "completed",
            "data_quality": "missing_start",
            "start_pickup_location_id": 93,
            "start_dropoff_location_id": 93,
            "end_dropoff_datetime": "2024-05-25 14:15:00",
            "end_fare_amount": 100.0,
            "end_tip_amount": 10.0,
            "end_trip_distance": 8.0,
        },
        {
            "trip_status": "started",
            "data_quality": "ok",
            "start_pickup_location_id": 93,
            "start_dropoff_location_id": 93,
            "end_dropoff_datetime": "2024-05-25 14:20:00",
            "end_fare_amount": 33.0,
            "end_tip_amount": 4.0,
            "end_trip_distance": 2.0,
        },
    ]

    result = aggregate_completed_trips(rows)

    assert len(result) == 1
    record = result[0]
    assert record["event_hour"] == "2024-05-25 14:00:00"
    assert record["trip_count"] == 2
    assert record["fare_sum"] == 60.5
    assert record["tip_sum"] == 3.0
    assert record["distance_sum"] == 2.5
    assert record["year"] == "2024"
    assert record["month"] == "05"
    assert record["day"] == "25"
    assert record["hour"] == "14"


def test_transform_top_routes_per_hour_ranks_routes_with_window_function():
    rows = [
        {
            "trip_status": "completed",
            "data_quality": "ok",
            "start_pickup_location_id": 10,
            "start_dropoff_location_id": 20,
            "end_dropoff_datetime": "2024-05-25 14:05:00",
            "end_fare_amount": 30.0,
            "end_tip_amount": 3.0,
            "end_trip_distance": 2.0,
        },
        {
            "trip_status": "completed",
            "data_quality": "ok",
            "start_pickup_location_id": 10,
            "start_dropoff_location_id": 20,
            "end_dropoff_datetime": "2024-05-25 14:15:00",
            "end_fare_amount": 20.0,
            "end_tip_amount": 2.0,
            "end_trip_distance": 1.0,
        },
        {
            "trip_status": "completed",
            "data_quality": "ok",
            "start_pickup_location_id": 11,
            "start_dropoff_location_id": 21,
            "end_dropoff_datetime": "2024-05-25 14:20:00",
            "end_fare_amount": 100.0,
            "end_tip_amount": 5.0,
            "end_trip_distance": 5.0,
        },
        {
            "trip_status": "completed",
            "data_quality": "ok",
            "start_pickup_location_id": 12,
            "start_dropoff_location_id": 22,
            "end_dropoff_datetime": "2024-05-25 15:10:00",
            "end_fare_amount": 60.0,
            "end_tip_amount": 6.0,
            "end_trip_distance": 3.0,
        },
        {
            "trip_status": "completed",
            "data_quality": "ok",
            "start_pickup_location_id": 13,
            "start_dropoff_location_id": 23,
            "end_dropoff_datetime": "2024-05-25 15:30:00",
            "end_fare_amount": 10.0,
            "end_tip_amount": 1.0,
            "end_trip_distance": 1.0,
        },
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
