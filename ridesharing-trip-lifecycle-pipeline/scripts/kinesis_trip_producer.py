"""Publish trip start and trip end CSV rows into separate Kinesis streams."""

import argparse
import csv
import json
import logging
import os
import time
from itertools import zip_longest

import boto3

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def read_rows(path):
    """
    Read CSV rows from path.

    Args:
        path: File path to CSV

    Returns:
        List of row dictionaries

    Example input:
        data/trip_start.csv

    Example output:
        [{"trip_id": "c66ce556bc", ...}, ...]
    """
    with open(path, "r", encoding="utf-8") as file_handle:
        return list(csv.DictReader(file_handle))


def make_chunks(rows, size):
    """Yield fixed-size chunks from row list."""
    for index in range(0, len(rows), size):
        yield rows[index : index + size]


def ensure_stream_active(client, stream_name):
    """
    Create stream if missing and wait for ACTIVE status.

    Args:
        client: boto3 Kinesis client
        stream_name: Kinesis stream name
    """
    try:
        client.describe_stream_summary(StreamName=stream_name)
    except client.exceptions.ResourceNotFoundException:
        log.info("Creating Kinesis stream '%s'", stream_name)
        client.create_stream(StreamName=stream_name, ShardCount=1)

    while True:
        status = client.describe_stream_summary(StreamName=stream_name)["StreamDescriptionSummary"]["StreamStatus"]
        if status == "ACTIVE":
            break
        log.info("Waiting for stream '%s' to become ACTIVE (current=%s)", stream_name, status)
        time.sleep(2)


def publish_batch(client, stream_name, rows):
    """
    Publish one batch of rows to Kinesis.

    Args:
        client: boto3 Kinesis client
        stream_name: Target stream name
        rows: Event rows to publish

    Returns:
        Count of successfully published records
    """
    if not rows:
        return 0

    records = [
        {
            "Data": json.dumps(row).encode("utf-8"),
            "PartitionKey": row["trip_id"],
        }
        for row in rows
    ]

    response = client.put_records(StreamName=stream_name, Records=records)
    failed_count = response.get("FailedRecordCount", 0)
    return len(rows) - failed_count


def publish_interleaved(client, start_stream, end_stream, start_rows, end_rows, batch_size, interval_seconds):
    """
    Publish start and end batches in alternating order.

    Args:
        client: boto3 Kinesis client
        start_stream: Stream for start events
        end_stream: Stream for end events
        start_rows: Start payload rows
        end_rows: End payload rows
        batch_size: Number of rows per put_records call
        interval_seconds: Pause between interleaved batches
    """
    sent_start = 0
    sent_end = 0

    start_batches = list(make_chunks(start_rows, batch_size))
    end_batches = list(make_chunks(end_rows, batch_size))

    for start_batch, end_batch in zip_longest(start_batches, end_batches, fillvalue=None):
        if start_batch:
            sent = publish_batch(client, start_stream, start_batch)
            sent_start += sent
            log.info("trip_start -> %s sent=%s total=%s", start_stream, sent, sent_start)

        if end_batch:
            sent = publish_batch(client, end_stream, end_batch)
            sent_end += sent
            log.info("trip_end -> %s sent=%s total=%s", end_stream, sent, sent_end)

        time.sleep(interval_seconds)

    log.info("Publishing complete. start=%s end=%s", sent_start, sent_end)


def parse_cli_args():
    """Parse producer CLI arguments."""
    parser = argparse.ArgumentParser(description="Publish trip CSV files to Kinesis streams")
    parser.add_argument("--start-file", default="data/trip_start.csv")
    parser.add_argument("--end-file", default="data/trip_end.csv")
    parser.add_argument("--start-stream", default=os.getenv("TRIP_START_STREAM", "trip-start-stream"))
    parser.add_argument("--end-stream", default=os.getenv("TRIP_END_STREAM", "trip-end-stream"))
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--interval-seconds", type=float, default=0.5)
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "ap-south-2"))
    parser.add_argument("--endpoint-url", default=os.getenv("KINESIS_ENDPOINT_URL") or os.getenv("AWS_ENDPOINT_URL"))
    return parser.parse_args()


def main():
    """Run producer in local or AWS mode."""
    args = parse_cli_args()

    client_options = {"region_name": args.region}
    if args.endpoint_url:
        client_options["endpoint_url"] = args.endpoint_url

    kinesis_client = boto3.client("kinesis", **client_options)
    ensure_stream_active(kinesis_client, args.start_stream)
    ensure_stream_active(kinesis_client, args.end_stream)

    start_rows = read_rows(args.start_file)
    end_rows = read_rows(args.end_file)

    log.info(
        "Starting publish with start_rows=%s end_rows=%s batch_size=%s endpoint=%s",
        len(start_rows),
        len(end_rows),
        args.batch_size,
        args.endpoint_url or "aws",
    )
    publish_interleaved(
        client=kinesis_client,
        start_stream=args.start_stream,
        end_stream=args.end_stream,
        start_rows=start_rows,
        end_rows=end_rows,
        batch_size=args.batch_size,
        interval_seconds=args.interval_seconds,
    )


if __name__ == "__main__":
    main()
