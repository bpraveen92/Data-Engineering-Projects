"""
Trip end Lambda: Kinesis payload -> DynamoDB update/upsert -> staged completed-trip write.

This module handles trip_end events. If a matching trip_start record is missing,
it writes a partial row and marks data_quality so downstream aggregation can filter.
"""

import base64
import json
import logging
import os
from datetime import datetime, timezone
from decimal import Decimal
from uuid import uuid4

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


def get_s3_client():
    """
    Create S3 client using env-aware endpoint.

    Returns:
        boto3 S3 client
    """
    endpoint_url = os.getenv("S3_ENDPOINT_URL") or os.getenv("AWS_ENDPOINT_URL")
    region_name = os.getenv("AWS_REGION", "ap-south-2")

    options = {"region_name": region_name}
    if endpoint_url:
        options["endpoint_url"] = endpoint_url

    return boto3.client("s3", **options)


def write_trip_end(table, payload, received_at):
    """
    Write trip_end payload and mark whether start row was missing.

    Args:
        table: DynamoDB lifecycle table
        payload: Validated trip_end payload
        received_at: Ingestion timestamp (ISO UTC)

    Returns:
        Dict with missing_start flag and current existing item snapshot

    Example output:
        {"missing_start": True, "existing_item": None}
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

    return {"missing_start": missing_start, "existing_item": existing_item}


def build_completed_trip_event(existing_item, payload, received_at):
    """
    Build compact completed-trip event for analytical staging.

    Args:
        existing_item: Existing DynamoDB lifecycle item with start-side fields
        payload: Validated trip_end payload
        received_at: Lambda processing timestamp

    Returns:
        Compact event dict or None when start-side fields are missing
    """
    if not existing_item:
        return None
    if existing_item.get("start_pickup_location_id") is None:
        return None
    if existing_item.get("start_dropoff_location_id") is None:
        return None

    return {
        "trip_id": payload["trip_id"],
        "pickup_location_id": int(existing_item["start_pickup_location_id"]),
        "dropoff_location_id": int(existing_item["start_dropoff_location_id"]),
        "dropoff_datetime": payload["dropoff_datetime"],
        "fare_amount": float(payload["fare_amount"]),
        "tip_amount": float(payload["tip_amount"]),
        "trip_distance": float(payload["trip_distance"]),
        "trip_status": "completed",
        "data_quality": "ok",
        "record_emitted_at": received_at,
        "record_source": "trip_end_update",
    }


def write_staging_records(s3_client, bucket_name, staging_prefix, records):
    """
    Write compact completed-trip events to append-only S3 staging files.

    Args:
        s3_client: boto3 S3 client
        bucket_name: Target S3 bucket
        staging_prefix: Base prefix for staged completed-trip events
        records: List of compact completed-trip event dicts
    """
    grouped_records = {}
    for record in records:
        event_time = datetime.fromisoformat(record["dropoff_datetime"])
        partition_prefix = (
            f"{staging_prefix.rstrip('/')}"
            f"/year={event_time.strftime('%Y')}"
            f"/month={event_time.strftime('%m')}"
            f"/day={event_time.strftime('%d')}"
            f"/hour={event_time.strftime('%H')}"
        )
        grouped_records.setdefault(partition_prefix, []).append(record)

    for partition_prefix, partition_records in grouped_records.items():
        object_key = (
            f"{partition_prefix}/completed_trip_batch_"
            f"{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S%fZ')}_"
            f"{uuid4().hex}.jsonl"
        )
        body_text = "\n".join(json.dumps(record) for record in partition_records)
        s3_client.put_object(
            Bucket=bucket_name,
            Key=object_key,
            Body=body_text.encode("utf-8"),
            ContentType="application/x-ndjson",
        )


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
    glue_arguments = {
        "--triggered_by": "trip_end_lambda",
        "--run_timestamp": run_timestamp,
    }

    top_routes_output_path = os.getenv("TOP_ROUTES_OUTPUT_PATH")
    top_routes_limit = os.getenv("TOP_ROUTES_LIMIT")
    staging_bucket = os.getenv("AGGREGATION_BUCKET")
    staging_prefix = os.getenv("COMPLETED_TRIP_STAGING_PREFIX")
    checkpoint_uri = os.getenv("AGGREGATION_CHECKPOINT_URI")
    aggregation_output_path = os.getenv("AGGREGATION_OUTPUT_PATH")
    if not aggregation_output_path and staging_bucket:
        aggregation_prefix = os.getenv("AGGREGATION_PREFIX", "aggregations/hourly_zone_metrics")
        aggregation_output_path = f"s3://{staging_bucket}/{aggregation_prefix.lstrip('/')}"

    if staging_bucket:
        glue_arguments["--staging_bucket"] = staging_bucket
    if staging_prefix:
        glue_arguments["--staging_prefix"] = staging_prefix
    if checkpoint_uri:
        glue_arguments["--checkpoint_uri"] = checkpoint_uri
    if aggregation_output_path:
        glue_arguments["--output_path"] = aggregation_output_path
    if top_routes_output_path:
        glue_arguments["--top_routes_output_path"] = top_routes_output_path
    if top_routes_limit:
        glue_arguments["--top_routes_limit"] = str(top_routes_limit)

    response = glue_client.start_job_run(
        JobName=job_name,
        Arguments=glue_arguments,
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
          "body": "{\"processed\": 10, \"failed\": 0, \"upserted_without_start\": 1, \"staged_completed_trips\": 9, ...}"
        }
    """
    del context

    table_name = os.getenv("TRIP_LIFECYCLE_TABLE", "trip_lifecycle")
    glue_job_name = os.getenv("GLUE_JOB_NAME", "")
    should_trigger_glue = os.getenv("TRIGGER_GLUE", "false").lower() == "true"
    staging_bucket = os.getenv("AGGREGATION_BUCKET", "")
    staging_prefix = os.getenv("COMPLETED_TRIP_STAGING_PREFIX", "staging/completed_trips")

    table = get_dynamo_table(table_name)
    glue_client = get_glue_client() if glue_job_name and should_trigger_glue else None
    s3_client = get_s3_client() if staging_bucket else None

    processed_count = 0
    failed_count = 0
    upserted_without_start = 0
    staged_count = 0
    staged_records = []

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
            received_at = now_iso_utc()
            write_result = write_trip_end(table, payload, received_at)
            processed_count += 1
            if write_result["missing_start"]:
                upserted_without_start += 1
            else:
                staged_event = build_completed_trip_event(write_result["existing_item"], payload, received_at)
                if staged_event:
                    staged_records.append(staged_event)
        except Exception:
            log.exception("Failed processing trip_end event for trip_id=%s", payload.get("trip_id"))
            failed_count += 1

    if staged_records and s3_client and staging_bucket:
        try:
            write_staging_records(s3_client, staging_bucket, staging_prefix, staged_records)
            staged_count = len(staged_records)
        except Exception:
            log.exception("Failed writing completed-trip staging records")

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
        "staged_completed_trips": staged_count,
        "table": table_name,
        "glue_job_name": glue_job_name or None,
        "glue_run_id": glue_run_id,
    }
    log.info("trip_end batch result=%s", result_body)
    return {"statusCode": 200, "body": json.dumps(result_body)}


if __name__ == "__main__":
    print("This module is intended for AWS Lambda runtime.")
