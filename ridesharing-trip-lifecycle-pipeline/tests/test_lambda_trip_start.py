import base64
import json

from scripts import lambda_trip_start


class FakeTable:
    def update_item(self, **kwargs):
        if not hasattr(self, "calls"):
            self.calls = []
        self.calls.append(kwargs)
        return {"ResponseMetadata": {"HTTPStatusCode": 200}}


def make_kinesis_record(payload):
    encoded = base64.b64encode(json.dumps(payload).encode("utf-8")).decode("utf-8")
    return {"kinesis": {"data": encoded}}


def test_validate_start_event_missing_fields():
    valid, missing = lambda_trip_start.validate_start_event({"trip_id": "abc"})
    assert valid is False
    assert "pickup_location_id" in missing


def test_write_trip_start_writes_expected_fields():
    table = FakeTable()
    table.calls = []
    payload = {
        "trip_id": "trip-1",
        "pickup_location_id": "10",
        "dropoff_location_id": "15",
        "vendor_id": "1",
        "pickup_datetime": "2024-05-25 01:00:00",
        "estimated_dropoff_datetime": "2024-05-25 01:20:00",
        "estimated_fare_amount": "12.34",
    }

    lambda_trip_start.write_trip_start(table, payload, "2024-05-25T01:00:00+00:00")

    assert len(table.calls) == 1
    call = table.calls[0]
    assert call["Key"] == {"trip_id": "trip-1"}
    assert call["ExpressionAttributeValues"][":pickup_loc"] == 10
    assert call["ExpressionAttributeValues"][":source"] == "trip_start"


def test_handler_processed_and_failed_counts(monkeypatch):
    table = FakeTable()
    table.calls = []
    monkeypatch.setattr(lambda_trip_start, "get_dynamo_table", lambda table_name: table)
    monkeypatch.setenv("TRIP_LIFECYCLE_TABLE", "trip_lifecycle")

    valid_payload = {
        "trip_id": "trip-1",
        "pickup_location_id": "10",
        "dropoff_location_id": "15",
        "vendor_id": "1",
        "pickup_datetime": "2024-05-25 01:00:00",
        "estimated_dropoff_datetime": "2024-05-25 01:20:00",
        "estimated_fare_amount": "12.34",
    }
    invalid_payload = {
        "trip_id": "trip-2",
        "pickup_location_id": "10",
    }

    event = {"Records": [make_kinesis_record(valid_payload), make_kinesis_record(invalid_payload)]}
    response = lambda_trip_start.handler(event, None)
    body = json.loads(response["body"])

    assert body["processed"] == 1
    assert body["failed"] == 1
