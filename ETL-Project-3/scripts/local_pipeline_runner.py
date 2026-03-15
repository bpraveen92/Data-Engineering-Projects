"""Run local end-to-end simulation: Kinesis -> Lambda handlers -> aggregation."""

import argparse
import base64
import json
import logging
import os

import boto3

from scripts.glue_trip_aggregator import run_local_staging_aggregation
from scripts.lambda_trip_end import handler as trip_end_handler
from scripts.lambda_trip_start import handler as trip_start_handler

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def fetch_stream_records(client, stream_name):
    """
    Fetch available records from all shards of one stream.

    Args:
        client: boto3 Kinesis client
        stream_name: Stream name

    Returns:
        List of raw Kinesis records

    Sample input data files (published earlier by producer):
        data/trip_start.csv -> trip-start-stream
        data/trip_end.csv   -> trip-end-stream

    Sample output:
        [
          {"Data": b'{"trip_id":"c66ce556bc", ...}', "PartitionKey": "c66ce556bc", ...},
          ...
        ]
    """
    stream_info = client.describe_stream(StreamName=stream_name)["StreamDescription"]
    shard_list = stream_info.get("Shards", [])
    all_records = []

    for shard in shard_list:
        shard_id = shard["ShardId"]
        iterator = client.get_shard_iterator(
            StreamName=stream_name,
            ShardId=shard_id,
            ShardIteratorType="TRIM_HORIZON",
        )["ShardIterator"]

        while iterator:
            response = client.get_records(ShardIterator=iterator, Limit=1000)
            records = response.get("Records", [])
            all_records.extend(records)
            iterator = response.get("NextShardIterator")
            if not records:
                break

    return all_records


def build_lambda_kinesis_event(records):
    """
    Convert raw Kinesis records into Lambda event shape.

    Args:
        records: Raw records returned by get_records

    Returns:
        Lambda Kinesis event payload

    Sample input:
        Records fetched from streams populated by:
        data/trip_start.csv and data/trip_end.csv

    Example output:
        {"Records": [{"kinesis": {"data": "<base64-json>"}}]}
    """
    return {
        "Records": [
            {
                "kinesis": {
                    "data": base64.b64encode(record["Data"]).decode("utf-8"),
                }
            }
            for record in records
        ]
    }


def run_in_batches(records, event_handler, batch_size):
    """
    Run Lambda-like handler on records in configured batch size.

    Args:
        records: Raw Kinesis records
        event_handler: Lambda handler callable
        batch_size: Batch size for each invocation

    Returns:
        List of handler response dicts

    Sample input source:
        data/trip_start.csv / data/trip_end.csv (via Kinesis records)

    Sample output:
        [
          {"statusCode": 200, "body": "{\"processed\": 100, \"failed\": 0, ...}"},
          ...
        ]
    """
    outputs = []
    for index in range(0, len(records), batch_size):
        chunk = records[index : index + batch_size]
        event = build_lambda_kinesis_event(chunk)
        outputs.append(event_handler(event, None))
    return outputs


def summarize_batch_output(handler_outputs):
    """
    Summarize processed/failed counters from handler outputs.

    Args:
        handler_outputs: Lambda-style response objects

    Returns:
        Dict with processed, failed, and staged counters

    Sample input:
        [{"statusCode": 200, "body": "{\"processed\": 100, \"failed\": 0, \"staged_completed_trips\": 98}"}]

    Sample output:
        {"processed": 100, "failed": 0, "staged_completed_trips": 98}
    """
    processed_count = 0
    failed_count = 0
    staged_count = 0
    for output in handler_outputs:
        body = json.loads(output.get("body", "{}"))
        processed_count += int(body.get("processed", 0))
        failed_count += int(body.get("failed", 0))
        staged_count += int(body.get("staged_completed_trips", 0))
    return {"processed": processed_count, "failed": failed_count, "staged_completed_trips": staged_count}


def parse_cli_args():
    """
    Parse local runner CLI arguments.

    Sample input files:
        --start-stream trip-start-stream (published from data/trip_start.csv)
        --end-stream trip-end-stream     (published from data/trip_end.csv)

    Sample output file path:
        --output-path ./output/aggregations
    """
    parser = argparse.ArgumentParser(description="Run ETL-Project-3 local pipeline simulation")
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "ap-south-2"))
    parser.add_argument("--endpoint-url", default=os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566"))
    parser.add_argument("--start-stream", default=os.getenv("TRIP_START_STREAM", "trip-start-stream"))
    parser.add_argument("--end-stream", default=os.getenv("TRIP_END_STREAM", "trip-end-stream"))
    parser.add_argument("--table-name", default=os.getenv("TRIP_LIFECYCLE_TABLE", "trip_lifecycle"))
    parser.add_argument("--staging-bucket", default=os.getenv("AGGREGATION_BUCKET", "etl-project-3-analytics-local"))
    parser.add_argument(
        "--staging-prefix",
        default=os.getenv("COMPLETED_TRIP_STAGING_PREFIX", "staging/completed_trips"),
    )
    parser.add_argument(
        "--checkpoint-uri",
        default=os.getenv(
            "AGGREGATION_CHECKPOINT_URI",
            "s3://etl-project-3-analytics-local/checkpoints/hourly_zone_metrics_checkpoint.json",
        ),
    )
    parser.add_argument("--output-path", default=os.getenv("LOCAL_OUTPUT_PATH", "./output/aggregations"))
    parser.add_argument("--batch-size", type=int, default=100)
    return parser.parse_args()


def main():
    """
    Run local simulation sequence and then run aggregation job.

    End-to-end sample data flow:
        1) data/trip_start.csv and data/trip_end.csv are published to Kinesis streams.
        2) Stream records are processed by Lambda handlers in batches.
        3) Aggregation output is written to:
           ./output/aggregations/year=YYYY/month=MM/day=DD/hour=HH/*.parquet

    Sample output files:
        output/aggregations/year=2024/month=05/day=25/hour=14/part-*.parquet
        output/aggregations/year=2024/month=05/day=26/hour=00/part-*.parquet
    """
    args = parse_cli_args()

    os.environ["AWS_REGION"] = args.region
    os.environ["AWS_ENDPOINT_URL"] = args.endpoint_url
    os.environ["DYNAMODB_ENDPOINT_URL"] = args.endpoint_url
    os.environ["S3_ENDPOINT_URL"] = args.endpoint_url
    os.environ["TRIP_LIFECYCLE_TABLE"] = args.table_name
    os.environ["TRIGGER_GLUE"] = "false"
    os.environ["AGGREGATION_BUCKET"] = args.staging_bucket
    os.environ["COMPLETED_TRIP_STAGING_PREFIX"] = args.staging_prefix
    os.environ["AGGREGATION_CHECKPOINT_URI"] = args.checkpoint_uri

    kinesis_client = boto3.client(
        "kinesis",
        region_name=args.region,
        endpoint_url=args.endpoint_url,
        aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID", "test"),
        aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    )

    start_records = fetch_stream_records(kinesis_client, args.start_stream)
    end_records = fetch_stream_records(kinesis_client, args.end_stream)
    log.info("Fetched stream records: trip_start=%s trip_end=%s", len(start_records), len(end_records))

    start_outputs = run_in_batches(start_records, trip_start_handler, args.batch_size)
    end_outputs = run_in_batches(end_records, trip_end_handler, args.batch_size)

    log.info("trip_start summary=%s", summarize_batch_output(start_outputs))
    log.info("trip_end summary=%s", summarize_batch_output(end_outputs))

    aggregation_summary = run_local_staging_aggregation(
        staging_bucket=args.staging_bucket,
        staging_prefix=args.staging_prefix,
        checkpoint_uri=args.checkpoint_uri,
        output_path=args.output_path,
        region=args.region,
        endpoint_url=args.endpoint_url,
        top_routes_output_path=os.getenv("TOP_ROUTES_OUTPUT_PATH"),
        top_routes_limit=int(os.getenv("TOP_ROUTES_LIMIT", "3")),
    )
    log.info("Aggregation summary=%s", aggregation_summary)
    log.info("Local pipeline completed. Aggregation output path=%s", args.output_path)


if __name__ == "__main__":
    main()
