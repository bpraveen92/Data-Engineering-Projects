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
    pass


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


def test_write_trip_end_upsert_when_missing_start():
    table = FakeTable()
    table.existing_item = None
    table.calls = []
    missing_start = lambda_trip_end.write_trip_end(table, sample_end_payload(), "2024-05-25T01:40:00+00:00")

    assert missing_start is True
    assert len(table.calls) == 1
    values = table.calls[0]["ExpressionAttributeValues"]
    assert values[":source"] == "trip_end_upsert"
    assert values[":data_quality"] == "missing_start"


def test_write_trip_end_update_when_start_exists():
    table = FakeTable()
    table.existing_item = {"trip_id": "trip-1", "start_pickup_location_id": 10}
    table.calls = []
    missing_start = lambda_trip_end.write_trip_end(table, sample_end_payload(), "2024-05-25T01:40:00+00:00")

    assert missing_start is False
    values = table.calls[0]["ExpressionAttributeValues"]
    assert values[":source"] == "trip_end_update"
    assert values[":data_quality"] == "ok"


def test_handler_triggers_glue(monkeypatch):
    table = FakeTable()
    table.existing_item = {"trip_id": "trip-1", "start_pickup_location_id": 10}
    table.calls = []
    monkeypatch.setattr(lambda_trip_end, "get_dynamo_table", lambda table_name: table)
    monkeypatch.setattr(lambda_trip_end, "get_glue_client", lambda: FakeGlueClient())
    monkeypatch.setattr(lambda_trip_end, "start_glue_job", lambda client, job_name, run_time: "jr_123")

    monkeypatch.setenv("TRIP_LIFECYCLE_TABLE", "trip_lifecycle")
    monkeypatch.setenv("GLUE_JOB_NAME", "etl-project-3-trip-aggregation")
    monkeypatch.setenv("TRIGGER_GLUE", "true")

    event = {"Records": [make_kinesis_record(sample_end_payload())]}
    response = lambda_trip_end.handler(event, None)
    body = json.loads(response["body"])

    assert body["processed"] == 1
    assert body["failed"] == 0
    assert body["glue_run_id"] == "jr_123"
