"""
Incremental aggregation for staged completed-trip events.

Execution modes:
- local_staging: boto3 reads staged files from LocalStack S3, then Spark aggregates locally
- glue_staging: boto3 lists staged files in S3, then AWS Glue Spark reads affected prefixes
"""

import argparse
import json
import logging
import os
from datetime import timezone
from decimal import Decimal

import boto3
from botocore.exceptions import ClientError
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

try:
    from awsglue.context import GlueContext
    from awsglue.job import Job
    from awsglue.utils import getResolvedOptions
except Exception:  # pragma: no cover
    GlueContext = None
    Job = None
    getResolvedOptions = None

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


def normalize_numeric_values(row):
    """
    Normalize one staged row for Spark DataFrame creation.

    Args:
        row: Raw dictionary from staged JSON or unit test input

    Returns:
        Dictionary where Decimal values are converted to float

    Sample input:
        {"trip_id": "t-1", "fare_amount": Decimal("40.5")}

    Sample output:
        {"trip_id": "t-1", "fare_amount": 40.5}
    """
    return {name: (float(value) if isinstance(value, Decimal) else value) for name, value in row.items()}


def resolve_runtime_option(argv, option_name, default_value=None):
    """
    Read optional argument from argv, supporting underscore and dash flag styles.

    Args:
        argv: Full argument list
        option_name: Base option name without leading dashes
        default_value: Value to return when option is not found

    Returns:
        String option value or default_value

    Sample input:
        argv = ["job.py", "--top_routes_limit", "5"]
        option_name = "top_routes_limit"

    Sample output:
        "5"
    """
    option_variants = [option_name, option_name.replace("_", "-")]
    for variant in option_variants:
        flag = f"--{variant}"
        for index, token in enumerate(argv):
            if token == flag and index + 1 < len(argv):
                return argv[index + 1]
            if token.startswith(flag + "="):
                return token.split("=", 1)[1]
    return default_value


def to_int_safe(value, default_value):
    """
    Convert value to int and fallback to default when conversion fails.

    Args:
        value: Raw value to convert
        default_value: Default integer when conversion fails

    Returns:
        Integer value
    """
    try:
        return int(value)
    except Exception:
        return int(default_value)


def parse_s3_uri(uri):
    """
    Split an S3 URI into bucket and key.

    Args:
        uri: S3 URI such as s3://bucket/path/file.json

    Returns:
        Tuple of (bucket_name, object_key)

    Sample input:
        "s3://etl-project-3-analytics-local/checkpoints/hourly.json"

    Sample output:
        ("etl-project-3-analytics-local", "checkpoints/hourly.json")
    """
    if not uri or not uri.startswith("s3://"):
        raise ValueError(f"Expected s3:// URI, got: {uri}")

    path_value = uri[5:]
    bucket_name, separator, object_key = path_value.partition("/")
    if not bucket_name or not separator or not object_key:
        raise ValueError(f"Expected full s3://bucket/key URI, got: {uri}")
    return bucket_name, object_key


def get_s3_client(region, endpoint_url=None):
    """
    Build an S3 client for local or AWS runs.

    Args:
        region: AWS region
        endpoint_url: Optional custom endpoint such as LocalStack

    Returns:
        boto3 S3 client
    """
    client_options = {"region_name": region}
    if endpoint_url:
        client_options["endpoint_url"] = endpoint_url
    return boto3.client("s3", **client_options)


def read_checkpoint_state(s3_client, checkpoint_uri):
    """
    Read incremental checkpoint metadata from S3.

    Args:
        s3_client: boto3 S3 client
        checkpoint_uri: S3 URI for checkpoint JSON

    Returns:
        Dict checkpoint state or None when checkpoint does not exist

    Sample output:
        {"last_modified": "2024-05-25T14:30:05+00:00", "last_key": "staging/.../batch.jsonl"}
    """
    if not checkpoint_uri:
        return None

    bucket_name, object_key = parse_s3_uri(checkpoint_uri)
    try:
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
    except ClientError as error:
        error_code = error.response.get("Error", {}).get("Code")
        if error_code in ["NoSuchKey", "404"]:
            return None
        raise

    body_text = response["Body"].read().decode("utf-8")
    return json.loads(body_text)


def write_checkpoint_state(s3_client, checkpoint_uri, checkpoint_state):
    """
    Persist incremental checkpoint metadata to S3.

    Args:
        s3_client: boto3 S3 client
        checkpoint_uri: S3 URI for checkpoint JSON
        checkpoint_state: Dict containing latest processed object marker
    """
    if not checkpoint_uri:
        return

    bucket_name, object_key = parse_s3_uri(checkpoint_uri)
    s3_client.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=json.dumps(checkpoint_state, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


def list_stage_objects(s3_client, bucket_name, staging_prefix):
    """
    List staged completed-trip objects under the configured prefix.

    Args:
        s3_client: boto3 S3 client
        bucket_name: S3 bucket containing staged files
        staging_prefix: Base staging prefix

    Returns:
        Sorted list of S3 object dictionaries
    """
    object_rows = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket_name, Prefix=staging_prefix.rstrip("/") + "/"):
        for row in page.get("Contents", []):
            if row.get("Size", 0) > 0:
                object_rows.append(row)

    object_rows.sort(key=lambda row: (row["LastModified"], row["Key"]))
    return object_rows


def is_object_after_checkpoint(object_row, checkpoint_state):
    """
    Check whether an S3 object is newer than the saved checkpoint.

    Args:
        object_row: S3 object row from list_objects_v2
        checkpoint_state: Saved checkpoint state dict

    Returns:
        True when object should be processed in the current run
    """
    if not checkpoint_state:
        return True

    last_modified = object_row["LastModified"].astimezone(timezone.utc).isoformat()
    last_key = object_row["Key"]
    checkpoint_modified = checkpoint_state.get("last_modified", "")
    checkpoint_key = checkpoint_state.get("last_key", "")
    return (last_modified, last_key) > (checkpoint_modified, checkpoint_key)


def select_new_stage_objects(object_rows, checkpoint_state):
    """
    Filter staged objects down to only the ones newer than checkpoint.

    Args:
        object_rows: Sorted S3 object rows
        checkpoint_state: Saved checkpoint state dict

    Returns:
        List of new S3 object rows
    """
    return [row for row in object_rows if is_object_after_checkpoint(row, checkpoint_state)]


def build_checkpoint_state(new_object_rows):
    """
    Build checkpoint metadata from newly processed stage objects.

    Args:
        new_object_rows: Sorted list of newly processed S3 object rows

    Returns:
        Dict checkpoint metadata or None when there were no new objects
    """
    if not new_object_rows:
        return None

    latest_row = new_object_rows[-1]
    return {
        "last_modified": latest_row["LastModified"].astimezone(timezone.utc).isoformat(),
        "last_key": latest_row["Key"],
        "processed_object_count": len(new_object_rows),
    }


def extract_hour_partition_prefix(object_key):
    """
    Extract hour partition prefix from one staged object key.

    Args:
        object_key: Full staged object key

    Returns:
        Partition prefix through hour=HH

    Sample input:
        staging/completed_trips/year=2024/month=05/day=25/hour=14/batch.jsonl

    Sample output:
        staging/completed_trips/year=2024/month=05/day=25/hour=14
    """
    key_parts = object_key.split("/")
    for index, part in enumerate(key_parts):
        if part.startswith("hour="):
            return "/".join(key_parts[: index + 1])
    raise ValueError(f"Could not find hour partition in key: {object_key}")


def collect_affected_partition_prefixes(new_object_rows):
    """
    Derive affected hour partitions from newly arrived staged files.

    Args:
        new_object_rows: List of newly arrived S3 object rows

    Returns:
        Sorted list of hour partition prefixes
    """
    return sorted({extract_hour_partition_prefix(row["Key"]) for row in new_object_rows})


def read_stage_records_from_s3(s3_client, bucket_name, partition_prefixes):
    """
    Read all staged JSONL records for the affected hour partitions.

    Args:
        s3_client: boto3 S3 client
        bucket_name: S3 bucket containing staged files
        partition_prefixes: List of hour partition prefixes

    Returns:
        List of staged trip dictionaries

    Sample output:
        [
          {
            "trip_id": "trip-1",
            "pickup_location_id": 93,
            "dropoff_location_id": 132,
            "dropoff_datetime": "2024-05-25 14:05:00",
            "fare_amount": 18.1
          }
        ]
    """
    records = []
    for prefix in partition_prefixes:
        for object_row in list_stage_objects(s3_client, bucket_name, prefix):
            response = s3_client.get_object(Bucket=bucket_name, Key=object_row["Key"])
            body_text = response["Body"].read().decode("utf-8")
            for line_text in body_text.splitlines():
                stripped_text = line_text.strip()
                if stripped_text:
                    records.append(json.loads(stripped_text))
    return records


def build_partition_uris(bucket_name, partition_prefixes):
    """
    Build S3 directory URIs for affected staged hour prefixes.

    Args:
        bucket_name: S3 bucket name
        partition_prefixes: List of hour partition prefixes

    Returns:
        List of s3:// URIs
    """
    return [f"s3://{bucket_name}/{prefix}" for prefix in partition_prefixes]


def deduplicate_completed_trips(frame):
    """
    Deduplicate staged completed-trip events by keeping the latest record per trip_id.

    Args:
        frame: Spark DataFrame built from staged JSON events

    Returns:
        Spark DataFrame with one latest row per trip_id
    """
    ranking_window = Window.partitionBy("trip_id").orderBy(
        F.col("record_emitted_at_ts").desc_nulls_last(),
        F.col("dropoff_datetime_ts").desc_nulls_last(),
        F.col("trip_id").asc(),
    )

    normalized_frame = (
        frame.filter(F.col("trip_status") == F.lit("completed"))
        .filter(F.coalesce(F.col("data_quality"), F.lit("ok")) != F.lit("missing_start"))
        .filter(F.col("pickup_location_id").isNotNull())
        .filter(F.col("dropoff_location_id").isNotNull())
        .filter(F.col("dropoff_datetime").isNotNull())
        .withColumn("pickup_location_id", F.col("pickup_location_id").cast("int"))
        .withColumn("dropoff_location_id", F.col("dropoff_location_id").cast("int"))
        .withColumn("fare_amount", F.col("fare_amount").cast("double"))
        .withColumn("tip_amount", F.col("tip_amount").cast("double"))
        .withColumn("trip_distance", F.col("trip_distance").cast("double"))
        .withColumn("dropoff_datetime_ts", F.to_timestamp("dropoff_datetime"))
        .withColumn("record_emitted_at_ts", F.to_timestamp("record_emitted_at"))
        .withColumn("row_rank", F.row_number().over(ranking_window))
        .filter(F.col("row_rank") == F.lit(1))
        .drop("row_rank")
    )
    return normalized_frame


def aggregate_completed_trips(records):
    """
    Aggregate completed staged rows to hourly metrics using native PySpark transforms.

    Args:
        records: Iterable of staged completed-trip dictionaries

    Returns:
        Sorted list of hourly metrics grouped by pickup_location_id,
        dropoff_location_id, and event_hour
    """
    row_list = list(records)
    if not row_list:
        return []

    active_session = SparkSession.getActiveSession()
    session_created_here = active_session is None
    spark = active_session or (
        SparkSession.builder.appName("etl-project-3-aggregate-completed-trips")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    try:
        source_frame = spark.createDataFrame([normalize_numeric_values(row) for row in row_list])
        result_rows = (
            transform_completed_trips(source_frame)
            .select(
                F.date_format("event_hour", "yyyy-MM-dd HH:mm:ss").alias("event_hour"),
                "pickup_location_id",
                "dropoff_location_id",
                "trip_count",
                F.round("fare_sum", 6).alias("fare_sum"),
                F.round("tip_sum", 6).alias("tip_sum"),
                F.round("distance_sum", 6).alias("distance_sum"),
                "year",
                "month",
                "day",
                "hour",
            )
            .orderBy("event_hour", "pickup_location_id", "dropoff_location_id")
            .collect()
        )
        return [row.asDict(recursive=True) for row in result_rows]
    finally:
        if session_created_here:
            spark.stop()


def transform_completed_trips(frame):
    """
    Transform staged completed-trip rows into hourly route metrics.

    Args:
        frame: Spark DataFrame built from staged completed-trip events

    Returns:
        Aggregated Spark DataFrame with partition columns
    """
    deduplicated_frame = deduplicate_completed_trips(frame)

    metric_frame = (
        deduplicated_frame.withColumn("event_hour", F.date_trunc("hour", F.col("dropoff_datetime_ts")))
        .groupBy("event_hour", "pickup_location_id", "dropoff_location_id")
        .agg(
            F.count(F.lit(1)).alias("trip_count"),
            F.sum("fare_amount").alias("fare_sum"),
            F.sum("tip_amount").alias("tip_sum"),
            F.sum("trip_distance").alias("distance_sum"),
        )
        .withColumn("year", F.date_format("event_hour", "yyyy"))
        .withColumn("month", F.date_format("event_hour", "MM"))
        .withColumn("day", F.date_format("event_hour", "dd"))
        .withColumn("hour", F.date_format("event_hour", "HH"))
    )
    return metric_frame


def transform_top_routes_per_hour(frame, top_limit=3):
    """
    Build top-N routes per hour using Spark window functions.

    Args:
        frame: Spark DataFrame with staged completed-trip columns
        top_limit: Number of top ranked routes to keep per hour

    Returns:
        Spark DataFrame with ranking and route quality metrics
    """
    metric_frame = transform_completed_trips(frame)
    ranking_window = Window.partitionBy("event_hour").orderBy(
        F.desc("trip_count"),
        F.desc("fare_sum"),
        F.desc("tip_sum"),
        F.asc("pickup_location_id"),
        F.asc("dropoff_location_id"),
    )

    top_limit_value = max(int(top_limit), 0)
    top_routes_frame = (
        metric_frame.withColumn("route_rank_in_hour", F.row_number().over(ranking_window))
        .filter(F.col("route_rank_in_hour") <= F.lit(top_limit_value))
        .withColumn("avg_fare_per_trip", F.round(F.col("fare_sum") / F.col("trip_count"), 6))
        .withColumn("avg_tip_per_trip", F.round(F.col("tip_sum") / F.col("trip_count"), 6))
        .withColumn(
            "tip_to_fare_ratio",
            F.round(
                F.when(F.col("fare_sum") > F.lit(0), F.col("tip_sum") / F.col("fare_sum")).otherwise(F.lit(0.0)),
                6,
            ),
        )
    )
    return top_routes_frame


def write_output(frame, output_path):
    """
    Write hourly metrics to parquet with dynamic partition overwrite.

    Args:
        frame: Aggregated Spark DataFrame
        output_path: S3 or local output path
    """
    frame.sparkSession.conf.set("spark.sql.sources.partitionOverwriteMode", "dynamic")
    (
        frame.coalesce(1)
        .write.mode("overwrite")
        .partitionBy("year", "month", "day", "hour")
        .parquet(output_path)
    )


def run_incremental_transforms(
    spark,
    source_frame,
    output_path,
    top_routes_output_path=None,
    top_routes_limit=3,
):
    """
    Run shared Spark transforms and write affected output partitions.

    Args:
        spark: Active Spark session
        source_frame: Spark DataFrame with staged completed-trip rows
        output_path: Parquet output root
        top_routes_output_path: Optional second output root
        top_routes_limit: Top-N routes per hour

    Returns:
        Summary dict for logging and tests
    """
    result_frame = transform_completed_trips(source_frame)
    affected_hours = result_frame.select("year", "month", "day", "hour").dropDuplicates().count()
    metric_rows = result_frame.count()
    write_output(result_frame, output_path)

    summary = {
        "metric_rows": metric_rows,
        "affected_hours": affected_hours,
        "top_route_rows": 0,
    }

    if top_routes_output_path:
        top_routes_frame = transform_top_routes_per_hour(source_frame, top_limit=top_routes_limit)
        summary["top_route_rows"] = top_routes_frame.count()
        write_output(top_routes_frame, top_routes_output_path)

    return summary


def run_local_staging_aggregation(
    staging_bucket,
    staging_prefix,
    checkpoint_uri,
    output_path,
    region,
    endpoint_url=None,
    top_routes_output_path=None,
    top_routes_limit=3,
):
    """
    Run incremental aggregation locally from staged S3 files.

    Args:
        staging_bucket: S3 bucket with completed-trip staging files
        staging_prefix: Base S3 prefix for staged files
        checkpoint_uri: S3 URI used to track last processed staged file
        output_path: Local parquet output root
        region: AWS region
        endpoint_url: LocalStack endpoint URL
    """
    s3_client = get_s3_client(region=region, endpoint_url=endpoint_url)
    checkpoint_state = read_checkpoint_state(s3_client, checkpoint_uri)
    all_objects = list_stage_objects(s3_client, staging_bucket, staging_prefix)
    new_objects = select_new_stage_objects(all_objects, checkpoint_state)

    if not new_objects:
        log.info("No new staged completed-trip files found after checkpoint. Nothing to aggregate.")
        return {"new_objects": 0, "affected_hours": 0, "metric_rows": 0, "top_route_rows": 0}

    affected_prefixes = collect_affected_partition_prefixes(new_objects)
    staged_records = read_stage_records_from_s3(s3_client, staging_bucket, affected_prefixes)
    if not staged_records:
        write_checkpoint_state(s3_client, checkpoint_uri, build_checkpoint_state(new_objects))
        log.info("New staged files were found, but no completed-trip rows were readable.")
        return {"new_objects": len(new_objects), "affected_hours": 0, "metric_rows": 0, "top_route_rows": 0}

    spark = (
        SparkSession.builder.appName("etl-project-3-trip-aggregation-local")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    try:
        source_frame = spark.createDataFrame([normalize_numeric_values(row) for row in staged_records])
        summary = run_incremental_transforms(
            spark=spark,
            source_frame=source_frame,
            output_path=output_path,
            top_routes_output_path=top_routes_output_path,
            top_routes_limit=top_routes_limit,
        )
    finally:
        spark.stop()

    write_checkpoint_state(s3_client, checkpoint_uri, build_checkpoint_state(new_objects))
    summary["new_objects"] = len(new_objects)
    log.info("Local incremental aggregation completed summary=%s", summary)
    return summary


def run_glue_staging_aggregation(
    staging_bucket,
    staging_prefix,
    checkpoint_uri,
    output_path,
    region,
    job_name,
    top_routes_output_path=None,
    top_routes_limit=3,
):
    """
    Run incremental aggregation on AWS Glue from staged S3 files.

    Args:
        staging_bucket: S3 bucket with completed-trip staging files
        staging_prefix: Base S3 prefix for staged files
        checkpoint_uri: S3 URI used to track last processed staged file
        output_path: S3 parquet output root
        region: AWS region
        job_name: Glue job name
    """
    if GlueContext is None:
        raise RuntimeError("awsglue package not available. This path must run on AWS Glue.")

    s3_client = get_s3_client(region=region)
    checkpoint_state = read_checkpoint_state(s3_client, checkpoint_uri)
    all_objects = list_stage_objects(s3_client, staging_bucket, staging_prefix)
    new_objects = select_new_stage_objects(all_objects, checkpoint_state)

    spark = SparkSession.builder.getOrCreate()
    glue_context = GlueContext(spark.sparkContext)
    job = Job(glue_context)
    job.init(job_name, {})

    if not new_objects:
        log.info("No new staged completed-trip files found after checkpoint. Glue run will exit cleanly.")
        job.commit()
        return {"new_objects": 0, "affected_hours": 0, "metric_rows": 0, "top_route_rows": 0}

    affected_prefixes = collect_affected_partition_prefixes(new_objects)
    partition_uris = build_partition_uris(staging_bucket, affected_prefixes)
    source_frame = spark.read.json(partition_uris)
    summary = run_incremental_transforms(
        spark=spark,
        source_frame=source_frame,
        output_path=output_path,
        top_routes_output_path=top_routes_output_path,
        top_routes_limit=top_routes_limit,
    )

    write_checkpoint_state(s3_client, checkpoint_uri, build_checkpoint_state(new_objects))
    summary["new_objects"] = len(new_objects)
    log.info("Glue incremental aggregation completed summary=%s", summary)
    job.commit()
    return summary


def parse_cli_args():
    """
    Parse CLI arguments for local and Glue execution modes.

    Returns:
        argparse namespace
    """
    parser = argparse.ArgumentParser(description="ETL-Project-3 trip aggregation job")
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
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "ap-south-2"))
    parser.add_argument("--endpoint-url", default=os.getenv("S3_ENDPOINT_URL") or os.getenv("AWS_ENDPOINT_URL"))
    parser.add_argument(
        "--mode",
        choices=["local_staging", "glue_staging"],
        default="local_staging",
        help="Use local_staging for local simulation and glue_staging for AWS Glue runtime.",
    )
    parser.add_argument("--job-name", default=os.getenv("GLUE_JOB_NAME", "etl-project-3-trip-aggregation"))
    parser.add_argument("--top-routes-output-path", default=os.getenv("TOP_ROUTES_OUTPUT_PATH"))
    parser.add_argument("--top-routes-limit", type=int, default=int(os.getenv("TOP_ROUTES_LIMIT", "3")))
    return parser.parse_args()


def main():
    """Main entry for Glue runtime and local CLI execution."""
    if getResolvedOptions and "JOB_NAME" in os.sys.argv:
        args = getResolvedOptions(
            os.sys.argv,
            ["JOB_NAME", "staging_bucket", "staging_prefix", "checkpoint_uri", "output_path", "region"],
        )
        top_routes_output_path = resolve_runtime_option(
            os.sys.argv,
            "top_routes_output_path",
            os.getenv("TOP_ROUTES_OUTPUT_PATH"),
        )
        top_routes_limit = to_int_safe(
            resolve_runtime_option(
                os.sys.argv,
                "top_routes_limit",
                os.getenv("TOP_ROUTES_LIMIT", "3"),
            ),
            3,
        )
        run_glue_staging_aggregation(
            staging_bucket=args["staging_bucket"],
            staging_prefix=args["staging_prefix"],
            checkpoint_uri=args["checkpoint_uri"],
            output_path=args["output_path"],
            region=args["region"],
            job_name=args["JOB_NAME"],
            top_routes_output_path=top_routes_output_path,
            top_routes_limit=top_routes_limit,
        )
        return

    args = parse_cli_args()
    if args.mode == "glue_staging":
        run_glue_staging_aggregation(
            staging_bucket=args.staging_bucket,
            staging_prefix=args.staging_prefix,
            checkpoint_uri=args.checkpoint_uri,
            output_path=args.output_path,
            region=args.region,
            job_name=args.job_name,
            top_routes_output_path=args.top_routes_output_path,
            top_routes_limit=args.top_routes_limit,
        )
    else:
        run_local_staging_aggregation(
            staging_bucket=args.staging_bucket,
            staging_prefix=args.staging_prefix,
            checkpoint_uri=args.checkpoint_uri,
            output_path=args.output_path,
            region=args.region,
            endpoint_url=args.endpoint_url,
            top_routes_output_path=args.top_routes_output_path,
            top_routes_limit=args.top_routes_limit,
        )


if __name__ == "__main__":
    main()
