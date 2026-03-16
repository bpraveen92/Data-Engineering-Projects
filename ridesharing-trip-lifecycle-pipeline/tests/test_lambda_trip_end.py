import base64
import json

from scripts import lambda_trip_end


class FakeTable:
    def get_item(self, **kwargs):
        del kwargs
        if getattr(self, "existing_item", None):
            return {"Item": self.existing_item}
        return {}

    def update_item(self, **kwargs):
        if not hasattr(self, "calls"):
            self.calls = []
        self.calls.append(kwargs)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}


class FakeGlueClient:
    def __init__(self):
        self.calls = []

    def start_job_run(self, **kwargs):
        self.calls.append(kwargs)
        return {"JobRunId": "jr_123"}


class FakeS3Client:
    def __init__(self):
        self.calls = []

    def put_object(self, **kwargs):
        self.calls.append(kwargs)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}


def make_kinesis_record(payload):
    encoded = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8")
    return {"kinesis": {"data": encoded}}


def sample_end_payload():
    return {
        "trip_id": "trip-1",
        "dropoff_datetime": "2024-05-25 01:35:00",
        "rate_code": "1.0",
        "passenger_count": "2.0",
        "trip_distance": "3.2",
        "fare_amount": "18.1",
        "tip_amount": "2.5",
        "payment_type": "1.0",
        "trip_type": "1.0",
    }


def sample_start_item():
    return {
        "trip_id": "trip-1",
        "start_pickup_location_id": 10,
        "start_dropoff_location_id": 20,
    }


def test_write_trip_end_upsert_when_missing_start():
    table = FakeTable()
    table.existing_item = None
    table.calls = []
    result = lambda_trip_end.write_trip_end(table, sample_end_payload(), "2024-05-25T01:40:00+00:00")

    assert result["missing_start"] is True
    assert result["existing_item"] is None
    assert len(table.calls) == 1
    values = table.calls[0]["ExpressionAttributeValues"]
    assert values[":source"] == "trip_end_upsert"
    assert values[":data_quality"] == "missing_start"


def test_write_trip_end_update_when_start_exists():
    table = FakeTable()
    table.existing_item = sample_start_item()
    table.calls = []
    result = lambda_trip_end.write_trip_end(table, sample_end_payload(), "2024-05-25T01:40:00+00:00")

    assert result["missing_start"] is False
    assert result["existing_item"]["start_pickup_location_id"] == 10
    values = table.calls[0]["ExpressionAttributeValues"]
    assert values[":source"] == "trip_end_update"
    assert values[":data_quality"] == "ok"


def test_build_completed_trip_event_returns_none_without_start_fields():
    event_row = lambda_trip_end.build_completed_trip_event(None, sample_end_payload(), "2024-05-25T01:40:00+00:00")

    assert event_row is None


def test_handler_stages_completed_trip(monkeypatch):
    table = FakeTable()
    table.existing_item = sample_start_item()
    table.calls = []
    s3_client = FakeS3Client()

    monkeypatch.setattr(lambda_trip_end, "get_dynamo_table", lambda table_name: table)
    monkeypatch.setattr(lambda_trip_end, "get_s3_client", lambda: s3_client)

    monkeypatch.setenv("TRIP_LIFECYCLE_TABLE", "trip_lifecycle")
    monkeypatch.setenv("AGGREGATION_BUCKET", "etl-project-3-analytics-local")
    monkeypatch.setenv("COMPLETED_TRIP_STAGING_PREFIX", "staging/completed_trips")
    monkeypatch.delenv("TRIGGER_GLUE", raising=False)

    event = {"Records": [make_kinesis_record(sample_end_payload())]}
    response = lambda_trip_end.handler(event, None)
    body = json.loads(response["body"])

    assert body["processed"] == 1
    assert body["failed"] == 0
    assert body["staged_completed_trips"] == 1
    assert body["glue_run_id"] is None
    assert len(s3_client.calls) == 1
    assert s3_client.calls[0]["Bucket"] == "etl-project-3-analytics-local"
    staged_body = s3_client.calls[0]["Body"].decode("utf-8")
    staged_rows = [json.loads(line_text) for line_text in staged_body.splitlines()]
    assert staged_rows[0]["trip_id"] == "trip-1"
    assert staged_rows[0]["pickup_location_id"] == 10
    assert staged_rows[0]["dropoff_location_id"] == 20


def test_handler_triggers_glue_with_incremental_args(monkeypatch):
    table = FakeTable()
    table.existing_item = sample_start_item()
    table.calls = []
    glue_client = FakeGlueClient()
    s3_client = FakeS3Client()

    monkeypatch.setattr(lambda_trip_end, "get_dynamo_table", lambda table_name: table)
    monkeypatch.setattr(lambda_trip_end, "get_glue_client", lambda: glue_client)
    monkeypatch.setattr(lambda_trip_end, "get_s3_client", lambda: s3_client)

    monkeypatch.setenv("TRIP_LIFECYCLE_TABLE", "trip_lifecycle")
    monkeypatch.setenv("GLUE_JOB_NAME", "etl-project-3-trip-aggregation")
    monkeypatch.setenv("TRIGGER_GLUE", "true")
    monkeypatch.setenv("AGGREGATION_BUCKET", "etl-project-3-analytics-local")
    monkeypatch.setenv("COMPLETED_TRIP_STAGING_PREFIX", "staging/completed_trips")
    monkeypatch.setenv(
        "AGGREGATION_CHECKPOINT_URI",
        "s3://etl-project-3-analytics-local/checkpoints/hourly_zone_metrics_checkpoint.json",
    )
    monkeypatch.setenv("AGGREGATION_PREFIX", "aggregations/hourly_zone_metrics")
    monkeypatch.setenv("TOP_ROUTES_LIMIT", "5")

    event = {"Records": [make_kinesis_record(sample_end_payload())]}
    response = lambda_trip_end.handler(event, None)
    body = json.loads(response["body"])

    assert body["processed"] == 1
    assert body["glue_run_id"] == "jr_123"
    assert glue_client.calls[0]["Arguments"]["--staging_bucket"] == "etl-project-3-analytics-local"
    assert glue_client.calls[0]["Arguments"]["--staging_prefix"] == "staging/completed_trips"
    assert (
        glue_client.calls[0]["Arguments"]["--checkpoint_uri"]
        == "s3://etl-project-3-analytics-local/checkpoints/hourly_zone_metrics_checkpoint.json"
    )
    assert (
        glue_client.calls[0]["Arguments"]["--output_path"]
        == "s3://etl-project-3-analytics-local/aggregations/hourly_zone_metrics"
    )
    assert glue_client.calls[0]["Arguments"]["--top_routes_limit"] == "5"
