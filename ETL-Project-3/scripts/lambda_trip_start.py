"""
Trip start Lambda: Kinesis payload -> DynamoDB state write.

This module handles trip_start events only. Each event is validated,
then written to DynamoDB by trip_id using deterministic updates.
"""

import base64
import json
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal

import boto3

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

required_fields = [
    "trip_id",
    "pickup_location_id",
    "dropoff_location_id",
    "vendor_id",
    "pickup_datetime",
    "estimated_dropoff_datetime",
    "estimated_fare_amount",
]


def now_iso_utc():
    """
    Return current UTC time as ISO string.

    Returns:
        Timestamp like 2026-03-14T12:30:45.123456+00:00
    """
    return datetime.now(timezone.utc).isoformat()


def parse_decimal(value):
    """
    Convert numeric input into Decimal for DynamoDB.

    Args:
        value: String/float/int numeric value

    Returns:
        Decimal value

    Example input:
        "34.18595283806629"

    Example output:
        Decimal('34.18595283806629')
    """
    return Decimal(str(value))


def decode_kinesis_record(record):
    """
    Decode one Kinesis record from base64 JSON.

    Args:
        record: One item from event['Records']

    Returns:
        Parsed dict payload or None

    Example input:
        {"kinesis": {"data": "eyJ0cmlwX2lkIjogImFiYzEyMyJ9"}}

    Example output:
        {"trip_id": "abc123"}
    """
    payload_text = record.get("kinesis", {}).get("data")
    if not payload_text:
        return None

    try:
        decoded_text = base64.b64decode(payload_text).decode("utf-8")
        return json.loads(decoded_text)
    except Exception:
        log.exception("Failed to decode Kinesis payload")
        return None


def validate_start_event(payload):
    """
    Validate required fields for trip_start payload.

    Args:
        payload: Decoded trip_start event payload

    Returns:
        (is_valid, missing_fields)

    Example output:
        (False, ['dropoff_location_id', 'vendor_id'])
    """
    missing_fields = [name for name in required_fields if payload.get(name) in (None, "")]
    return len(missing_fields) == 0, missing_fields


def get_dynamo_table(table_name):
    """
    Create DynamoDB table handle using env-aware endpoint.

    Args:
        table_name: DynamoDB table name

    Returns:
        boto3 DynamoDB table resource
    """
    endpoint_url = os.getenv("DYNAMODB_ENDPOINT_URL") or os.getenv("AWS_ENDPOINT_URL")
    region_name = os.getenv("AWS_REGION", "ap-south-2")

    options = {"region_name": region_name}
    if endpoint_url:
        options["endpoint_url"] = endpoint_url

    resource = boto3.resource("dynamodb", **options)
    return resource.Table(table_name)


def write_trip_start(table, payload, received_at):
    """
    Write one trip_start event into DynamoDB.

    Args:
        table: DynamoDB table handle
        payload: Validated trip_start payload
        received_at: Ingestion timestamp (ISO UTC)

    Example input:
        payload['trip_id'] = 'c66ce556bc'

    Example output:
        DynamoDB start_* fields are updated, trip_status=started is set if absent
    """
    table.update_item(
        Key={"trip_id": payload["trip_id"]},
        UpdateExpression=(
            "SET start_pickup_location_id = :pickup_loc, "
            "start_dropoff_location_id = :dropoff_loc, "
            "start_vendor_id = :vendor_id, "
            "start_pickup_datetime = :pickup_time, "
            "start_estimated_dropoff_datetime = :estimated_dropoff_time, "
            "start_estimated_fare_amount = :estimated_fare, "
            "start_received_at = :received_at, "
            "updated_at = :updated_at, "
            "trip_status = if_not_exists(trip_status, :started), "
            "record_source = :source"
        ),
        ExpressionAttributeValues={
            ":pickup_loc": int(payload["pickup_location_id"]),
            ":dropoff_loc": int(payload["dropoff_location_id"]),
            ":vendor_id": int(payload["vendor_id"]),
            ":pickup_time": payload["pickup_datetime"],
            ":estimated_dropoff_time": payload["estimated_dropoff_datetime"],
            ":estimated_fare": parse_decimal(payload["estimated_fare_amount"]),
            ":received_at": received_at,
            ":updated_at": received_at,
            ":started": "started",
            ":source": "trip_start",
        },
        ReturnValues="NONE",
    )


def handler(event, context):
    """
    Process Lambda batch for trip_start stream.

    Args:
        event: Lambda event with Records list from Kinesis
        context: Lambda context object (unused)

    Returns:
        dict with statusCode and JSON body

    Example input:
        {"Records": [{"kinesis": {"data": "<base64-json>"}}]}

    Example output:
        {"statusCode": 200, "body": "{\"processed\": 1, \"failed\": 0, ...}"}
    """
    del context

    table_name = os.getenv("TRIP_LIFECYCLE_TABLE", "trip_lifecycle")
    table = get_dynamo_table(table_name)

    processed_count = 0
    failed_count = 0

    for record in event.get("Records", []):
        payload = decode_kinesis_record(record)
        if not payload:
            failed_count += 1
            continue

        is_valid, missing_fields = validate_start_event(payload)
        if not is_valid:
            log.warning("Skipping invalid trip_start event; missing=%s payload=%s", missing_fields, payload)
            failed_count += 1
            continue

        try:
            write_trip_start(table, payload, now_iso_utc())
            processed_count += 1
        except Exception:
            log.exception("Failed processing trip_start event for trip_id=%s", payload.get("trip_id"))
            failed_count += 1

    result_body = {
        "processed": processed_count,
        "failed": failed_count,
        "table": table_name,
    }
    log.info("trip_start batch result=%s", result_body)
    return {"statusCode": 200, "body": json.dumps(result_body)}


if __name__ == "__main__":
    print("This module is intended for AWS Lambda runtime.")
