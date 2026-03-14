"""
Trip end Lambda: Kinesis payload -> DynamoDB update/upsert -> Glue trigger.

This module handles trip_end events. If a matching trip_start record is missing,
it writes a partial row and marks data_quality so downstream aggregation can filter.
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
    "dropoff_datetime",
    "rate_code",
    "passenger_count",
    "trip_distance",
    "fare_amount",
    "tip_amount",
    "payment_type",
    "trip_type",
]


def now_iso_utc():
    """Return current UTC time as ISO string."""
    return datetime.now(timezone.utc).isoformat()


def parse_decimal(value):
    """
    Convert numeric input into Decimal for DynamoDB fields.

    Args:
        value: Any numeric value from payload

    Returns:
        Decimal value
    """
    return Decimal(str(value))


def decode_kinesis_record(record):
    """
    Decode one Kinesis record into JSON payload.

    Args:
        record: One Lambda Kinesis record

    Returns:
        Parsed dict payload or None

    Example output:
        {"trip_id": "c66ce556bc"}
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


def validate_end_event(payload):
    """
    Validate required fields for trip_end payload.

    Args:
        payload: Decoded trip_end payload

    Returns:
        (is_valid, missing_fields)
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


def get_glue_client():
    """
    Create Glue client using env-aware endpoint.

    Returns:
        boto3 Glue client
    """
    endpoint_url = os.getenv("GLUE_ENDPOINT_URL") or os.getenv("AWS_ENDPOINT_URL")
    region_name = os.getenv("AWS_REGION", "ap-south-2")

    options = {"region_name": region_name}
    if endpoint_url:
        options["endpoint_url"] = endpoint_url

    return boto3.client("glue", **options)


def write_trip_end(table, payload, received_at):
    """
    Write trip_end payload and mark whether start row was missing.

    Args:
        table: DynamoDB lifecycle table
        payload: Validated trip_end payload
        received_at: Ingestion timestamp (ISO UTC)

    Returns:
        True if start record was missing and row was upserted

    Example output:
        True  # means record_source=trip_end_upsert, data_quality=missing_start
    """
    trip_id = payload["trip_id"]
    existing_item = table.get_item(Key={"trip_id": trip_id}).get("Item")

    missing_start = not existing_item or not existing_item.get("start_pickup_location_id")
    source_value = "trip_end_upsert" if missing_start else "trip_end_update"

    table.update_item(
        Key={"trip_id": trip_id},
        UpdateExpression=(
            "SET end_dropoff_datetime = :dropoff_time, "
            "end_rate_code = :rate_code, "
            "end_passenger_count = :passenger_count, "
            "end_trip_distance = :trip_distance, "
            "end_fare_amount = :fare_amount, "
            "end_tip_amount = :tip_amount, "
            "end_payment_type = :payment_type, "
            "end_trip_type = :trip_type, "
            "end_received_at = :received_at, "
            "updated_at = :updated_at, "
            "trip_status = :trip_status, "
            "data_quality = :data_quality, "
            "record_source = :source"
        ),
        ExpressionAttributeValues={
            ":dropoff_time": payload["dropoff_datetime"],
            ":rate_code": parse_decimal(payload["rate_code"]),
            ":passenger_count": parse_decimal(payload["passenger_count"]),
            ":trip_distance": parse_decimal(payload["trip_distance"]),
            ":fare_amount": parse_decimal(payload["fare_amount"]),
            ":tip_amount": parse_decimal(payload["tip_amount"]),
            ":payment_type": parse_decimal(payload["payment_type"]),
            ":trip_type": parse_decimal(payload["trip_type"]),
            ":received_at": received_at,
            ":updated_at": received_at,
            ":trip_status": "completed",
            ":data_quality": "missing_start" if missing_start else "ok",
            ":source": source_value,
        },
        ReturnValues="NONE",
    )

    return missing_start


def start_glue_job(glue_client, job_name, run_timestamp):
    """
    Trigger Glue aggregation job from Lambda.

    Args:
        glue_client: boto3 Glue client
        job_name: Glue job name
        run_timestamp: ISO timestamp passed to Glue args

    Returns:
        Glue JobRunId string
    """
    response = glue_client.start_job_run(
        JobName=job_name,
        Arguments={
            "--triggered_by": "trip_end_lambda",
            "--run_timestamp": run_timestamp,
        },
    )
    return response["JobRunId"]


def handler(event, context):
    """
    Process Lambda batch for trip_end stream and optionally trigger Glue.

    Args:
        event: Lambda event with Kinesis records
        context: Lambda context object (unused)

    Returns:
        dict with statusCode and JSON body summary

    Example output:
        {
          "statusCode": 200,
          "body": "{\"processed\": 10, \"failed\": 0, \"upserted_without_start\": 1, ...}"
        }
    """
    del context

    table_name = os.getenv("TRIP_LIFECYCLE_TABLE", "trip_lifecycle")
    glue_job_name = os.getenv("GLUE_JOB_NAME", "")
    should_trigger_glue = os.getenv("TRIGGER_GLUE", "true").lower() == "true"

    table = get_dynamo_table(table_name)
    glue_client = get_glue_client() if glue_job_name and should_trigger_glue else None

    processed_count = 0
    failed_count = 0
    upserted_without_start = 0

    for record in event.get("Records", []):
        payload = decode_kinesis_record(record)
        if not payload:
            failed_count += 1
            continue

        is_valid, missing_fields = validate_end_event(payload)
        if not is_valid:
            log.warning("Skipping invalid trip_end event; missing=%s payload=%s", missing_fields, payload)
            failed_count += 1
            continue

        try:
            was_upsert = write_trip_end(table, payload, now_iso_utc())
            processed_count += 1
            if was_upsert:
                upserted_without_start += 1
        except Exception:
            log.exception("Failed processing trip_end event for trip_id=%s", payload.get("trip_id"))
            failed_count += 1

    glue_run_id = None
    if processed_count > 0 and glue_client and glue_job_name:
        try:
            glue_run_id = start_glue_job(glue_client, glue_job_name, now_iso_utc())
        except Exception:
            log.exception("Failed to trigger Glue job '%s'", glue_job_name)

    result_body = {
        "processed": processed_count,
        "failed": failed_count,
        "upserted_without_start": upserted_without_start,
        "table": table_name,
        "glue_job_name": glue_job_name or None,
        "glue_run_id": glue_run_id,
    }
    log.info("trip_end batch result=%s", result_body)
    return {"statusCode": 200, "body": json.dumps(result_body)}


if __name__ == "__main__":
    print("This module is intended for AWS Lambda runtime.")
