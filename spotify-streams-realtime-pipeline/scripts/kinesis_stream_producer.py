#!/usr/bin/env python3
"""
Kinesis Stream Producer for the Spotify streams real-time pipeline

Reads sample streaming data from CSV and generates synthetic events,
sending them to AWS Kinesis (or LocalStack for local testing).

This simulates a continuous stream of music listening events from users
across different regions and devices.
"""

import json
import logging
import csv
import time
import argparse
import sys
import os
import random
from datetime import datetime

import boto3
from botocore.exceptions import ClientError

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not available in AWS Glue runtime — env vars come from job parameters

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


def load_csv(csv_file):
    """
    Load streaming events from a CSV file.

    Args:
        csv_file: Path to the CSV file.

    Returns:
        List of row dicts (one per event).
    """
    events = []
    try:
        with open(csv_file, 'r') as f:
            reader = csv.DictReader(f)
            for row in reader:
                events.append(row)
        logger.info(f"Loaded {len(events)} sample events from {csv_file}")
        return events
    except FileNotFoundError:
        logger.error(f"CSV file not found: {csv_file}")
        raise


def initialize_kinesis_client(region, use_localstack):
    """
    Initialize and return a Boto3 Kinesis client.

    Args:
        region: AWS region name.
        use_localstack: If True, points the client at the LocalStack endpoint.

    Returns:
        Boto3 Kinesis client.
    """
    kwargs = {'region_name': region}

    if use_localstack:
        kwargs['endpoint_url'] = os.getenv(
            'KINESIS_ENDPOINT', 'http://localhost:4566')
        os.environ['AWS_ACCESS_KEY_ID'] = os.getenv(
            'AWS_ACCESS_KEY_ID', 'test')
        os.environ['AWS_SECRET_ACCESS_KEY'] = os.getenv(
            'AWS_SECRET_ACCESS_KEY', 'test')
    else:
        # For production: read from .env or AWS credentials chain
        access_key = os.getenv('AWS_ACCESS_KEY_ID')
        secret_key = os.getenv('AWS_SECRET_ACCESS_KEY')

        if access_key and secret_key:
            os.environ['AWS_ACCESS_KEY_ID'] = access_key
            os.environ['AWS_SECRET_ACCESS_KEY'] = secret_key

    try:
        kinesis = boto3.client('kinesis', **kwargs)
        endpoint = kwargs.get('endpoint_url', 'AWS')
        logger.info(f"Connected to Kinesis (endpoint: {endpoint})")
        return kinesis
    except ClientError as e:
        logger.error(f"Failed to connect to Kinesis: {e}")
        raise


def ensure_stream_exists(kinesis, stream_name):
    """
    Check if a Kinesis stream exists and create it if not.

    Args:
        kinesis: Boto3 Kinesis client.
        stream_name: Stream name to check/create.
    """
    try:
        # Try to describe the stream
        response = kinesis.describe_stream(StreamName=stream_name)
        logger.info(f"Stream '{stream_name}' already exists")
        return
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            # Stream doesn't exist, create it
            logger.info(f"Creating Kinesis stream '{stream_name}'...")
            try:
                kinesis.create_stream(StreamName=stream_name, ShardCount=1)
                logger.info(f"Stream '{stream_name}' created successfully!")

                # Wait for stream to be active
                waiter = kinesis.get_waiter('stream_exists')
                waiter.wait(StreamName=stream_name)
                logger.info(f"Stream '{stream_name}' is now active")
            except ClientError as create_err:
                logger.error(f"Failed to create stream: {create_err}")
                raise
        else:
            logger.error(f"Failed to check stream status: {e}")
            raise


def generate_event(base_event):
    """
    Enrich a CSV row with a current UTC timestamp and fixed event metadata.

    Args:
        base_event: Dict with user_id and track_id from CSV.

    Returns:
        Event dict ready to be JSON-serialised and sent to Kinesis.
    """
    return {
        'user_id': base_event['user_id'],
        'track_id': base_event['track_id'],
        'timestamp': datetime.utcnow().isoformat(),
        'event_type': 'stream_event',
        'source': 'mobile_app'
    }


def send_batch(kinesis, stream_name, events, batch_size):
    """
    Sample and send a random batch of events to Kinesis.

    Args:
        kinesis: Boto3 Kinesis client.
        stream_name: Target stream name.
        events: Full list of loaded CSV events to sample from.
        batch_size: Number of events to send in this batch.

    Returns:
        Number of events successfully sent.
    """
    if not events:
        logger.error("No events loaded")
        return 0

    # Sample random events
    batch = random.sample(events, min(batch_size, len(events)))
    sent = 0

    for event in batch:
        synthetic = generate_event(event)
        try:
            kinesis.put_record(
                StreamName=stream_name,
                Data=json.dumps(synthetic),
                PartitionKey=event['user_id']  # Keeps user events ordered
            )
            sent += 1
        except ClientError as e:
            logger.error(f"Failed to send record: {e}")

    return sent


def run_producer(kinesis, stream_name, events, batch_size, interval_seconds, duration_seconds=None, max_records=None):
    """
    Run the producer loop until a stop condition is reached or KeyboardInterrupt.

    Args:
        kinesis: Boto3 Kinesis client.
        stream_name: Target stream name.
        events: Full list of CSV events to sample from.
        batch_size: Events per batch.
        interval_seconds: Sleep between batches.
        duration_seconds: Stop after this many seconds (optional).
        max_records: Stop after sending this many records (optional).
    """
    logger.info(f"Starting producer: {stream_name}")
    logger.info(f"Batch size: {batch_size}, Interval: {interval_seconds}s")

    start_time = time.time()
    total_sent = 0
    batch_num = 0

    try:
        while True:
            # Check stop conditions
            if max_records and total_sent >= max_records:
                logger.info(f"Reached max_records: {max_records}")
                break

            elapsed = time.time() - start_time
            if duration_seconds and elapsed > duration_seconds:
                logger.info(f"Reached duration: {duration_seconds}s")
                break

            batch_num += 1
            sent = send_batch(kinesis, stream_name, events, batch_size)
            total_sent += sent

            logger.info(
                f"[Batch {batch_num}] Sent {sent} events (total: {total_sent})")
            time.sleep(interval_seconds)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        elapsed = time.time() - start_time
        logger.info(
            f"\nProducer stopped. Sent {total_sent} events in {elapsed:.1f}s")


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Send synthetic music streaming events to Kinesis'
    )
    parser.add_argument('--stream-name', required=True,
                        help='Kinesis stream name')
    parser.add_argument('--region', default='ap-south-2', help='AWS region')
    parser.add_argument(
        '--csv-file', default='sample_data_initial_load/streams.csv')
    parser.add_argument('--batch-size', type=int,
                        default=10, help='Events per batch')
    parser.add_argument('--interval-seconds', type=float, default=5.0)
    parser.add_argument('--duration-seconds', type=int,
                        help='Total runtime (optional)')
    parser.add_argument('--max-records', type=int,
                        help='Max events to send (optional)')
    parser.add_argument('--local', action='store_true',
                        help='Use LocalStack (local testing)')

    args = parser.parse_args()

    try:
        # Load events from CSV
        events = load_csv(args.csv_file)

        # Initialize Kinesis client
        kinesis = initialize_kinesis_client(args.region, args.local)

        # Ensure stream exists
        ensure_stream_exists(kinesis, args.stream_name)

        # Run producer
        run_producer(
            kinesis=kinesis,
            stream_name=args.stream_name,
            events=events,
            batch_size=args.batch_size,
            interval_seconds=args.interval_seconds,
            duration_seconds=args.duration_seconds,
            max_records=args.max_records
        )
    except Exception as e:
        logger.error(f"Producer failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
