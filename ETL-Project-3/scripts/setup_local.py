"""Create local resources in LocalStack for ETL-Project-3."""

import argparse
import logging
import os
import time

import boto3
from botocore.exceptions import ClientError

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO,
                    format="%(asctime)s %(levelname)s %(message)s")


def ensure_stream_active(client, stream_name):
    """Create stream if needed and block until ACTIVE."""
    try:
        client.describe_stream_summary(StreamName=stream_name)
        log.info("Kinesis stream already exists: %s", stream_name)
    except client.exceptions.ResourceNotFoundException:
        log.info("Creating Kinesis stream: %s", stream_name)
        client.create_stream(StreamName=stream_name, ShardCount=1)

    while True:
        status = client.describe_stream_summary(StreamName=stream_name)[
            "StreamDescriptionSummary"]["StreamStatus"]
        if status == "ACTIVE":
            break
        time.sleep(2)
        log.info("Waiting for stream %s to become ACTIVE (current=%s)",
                 stream_name, status)


def ensure_table_exists(dynamo_client, table_name):
    """Create the trip_lifecycle DynamoDB table if it doesn't already exist."""
    try:
        dynamo_client.describe_table(TableName=table_name)
        log.info("DynamoDB table already exists: %s", table_name)
        return
    except dynamo_client.exceptions.ResourceNotFoundException:
        pass

    log.info("Creating DynamoDB table: %s", table_name)
    dynamo_client.create_table(
        TableName=table_name,
        AttributeDefinitions=[
            {"AttributeName": "trip_id", "AttributeType": "S"}],
        KeySchema=[{"AttributeName": "trip_id", "KeyType": "HASH"}],
        BillingMode="PAY_PER_REQUEST",
    )

    waiter = dynamo_client.get_waiter("table_exists")
    waiter.wait(TableName=table_name)


def ensure_bucket_exists(s3_client, bucket_name, region):
    """Create the S3 bucket if it doesn't already exist."""
    try:
        s3_client.head_bucket(Bucket=bucket_name)
        log.info("S3 bucket already exists: %s", bucket_name)
        return
    except ClientError:
        pass

    log.info("Creating S3 bucket: %s", bucket_name)
    if region == "us-east-1":
        s3_client.create_bucket(Bucket=bucket_name)
    else:
        s3_client.create_bucket(
            Bucket=bucket_name,
            CreateBucketConfiguration={"LocationConstraint": region},
        )


def ensure_glue_database_exists(glue_client, database_name):
    """Create the Glue database if it doesn't already exist. No-ops silently if Glue is unavailable (LocalStack community)."""
    try:
        glue_client.get_database(Name=database_name)
        log.info("Glue database already exists: %s", database_name)
        return
    except glue_client.exceptions.EntityNotFoundException:
        pass
    except Exception as error:
        log.warning("Skipping Glue database check: %s", error)
        return

    try:
        glue_client.create_database(DatabaseInput={"Name": database_name})
        log.info("Glue database created: %s", database_name)
    except Exception as error:
        log.warning(
            "Skipping Glue database creation (unsupported in current local setup): %s", error)


def parse_cli_args():
    """Parse setup script CLI arguments."""
    parser = argparse.ArgumentParser(
        description="Create LocalStack resources for ETL-Project-3")
    parser.add_argument(
        "--region", default=os.getenv("AWS_REGION", "ap-south-2"))
    parser.add_argument(
        "--endpoint-url", default=os.getenv("AWS_ENDPOINT_URL", "http://localhost:4566"))
    parser.add_argument(
        "--start-stream", default=os.getenv("TRIP_START_STREAM", "trip-start-stream"))
    parser.add_argument(
        "--end-stream", default=os.getenv("TRIP_END_STREAM", "trip-end-stream"))
    parser.add_argument(
        "--table-name", default=os.getenv("TRIP_LIFECYCLE_TABLE", "trip_lifecycle"))
    parser.add_argument("--output-bucket", default=os.getenv(
        "AGGREGATION_BUCKET", "etl-project-3-analytics-local"))
    parser.add_argument(
        "--glue-database", default=os.getenv("GLUE_DATABASE", "etl_project_3_analytics"))
    return parser.parse_args()


def main():
    """Create required local resources for stream simulation flow."""
    args = parse_cli_args()

    client_options = {
        "region_name": args.region,
        "endpoint_url": args.endpoint_url,
        "aws_access_key_id": os.getenv("AWS_ACCESS_KEY_ID", "test"),
        "aws_secret_access_key": os.getenv("AWS_SECRET_ACCESS_KEY", "test"),
    }

    kinesis_client = boto3.client("kinesis", **client_options)
    dynamo_client = boto3.client("dynamodb", **client_options)
    s3_client = boto3.client("s3", **client_options)
    glue_client = boto3.client("glue", **client_options)

    ensure_stream_active(kinesis_client, args.start_stream)
    ensure_stream_active(kinesis_client, args.end_stream)
    ensure_table_exists(dynamo_client, args.table_name)
    ensure_bucket_exists(s3_client, args.output_bucket, args.region)
    ensure_glue_database_exists(glue_client, args.glue_database)

    log.info("Local setup complete.")
    log.info("Streams: %s, %s", args.start_stream, args.end_stream)
    log.info("Table: %s", args.table_name)
    log.info("Bucket: %s", args.output_bucket)


if __name__ == "__main__":
    main()
