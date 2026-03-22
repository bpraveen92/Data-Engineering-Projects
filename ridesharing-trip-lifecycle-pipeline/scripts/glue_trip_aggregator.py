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
import re
from datetime import datetime, timedelta, timezone
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
    Convert Decimal values to float so Spark can create a DataFrame from staged rows.

    DynamoDB and some boto3 deserializers return numeric fields as Decimal, which
    PySpark's createDataFrame does not accept.

    Input:  {"trip_id": "t-1", "fare_amount": Decimal("40.50"), "tip_amount": Decimal("2.0")}
    Output: {"trip_id": "t-1", "fare_amount": 40.5,            "tip_amount": 2.0}
    """
    return {k: (float(v) if isinstance(v, Decimal) else v) for k, v in row.items()}


def resolve_runtime_option(argv, option_name, default_value=None):
    """Read an optional arg from argv, accepting both underscore and dash flag styles."""
    for variant in [option_name, option_name.replace("_", "-")]:
        flag = f"--{variant}"
        for i, token in enumerate(argv):
            if token == flag and i + 1 < len(argv):
                return argv[i + 1]
            if token.startswith(flag + "="):
                return token.split("=", 1)[1]
    return default_value


def to_int_safe(value, default_value):
    """Convert value to int, falling back to default_value on failure."""
    try:
        return int(value)
    except Exception:
        return int(default_value)


def parse_s3_uri(uri):
    """
    Split an S3 URI into (bucket, key). Raises ValueError for malformed URIs.

    "s3://etl-project-3-analytics-local/checkpoints/hourly_zone_metrics_checkpoint.json"
     → ("etl-project-3-analytics-local", "checkpoints/hourly_zone_metrics_checkpoint.json")
    """
    if not uri or not uri.startswith("s3://"):
        raise ValueError(f"Expected s3:// URI, got: {uri}")
    bucket, sep, key = uri[5:].partition("/")
    if not bucket or not sep or not key:
        raise ValueError(f"Expected full s3://bucket/key URI, got: {uri}")
    return bucket, key


def get_s3_client(region, endpoint_url=None):
    """Build an S3 client, optionally pointed at a custom endpoint (e.g. LocalStack)."""
    kwargs = {"region_name": region}
    if endpoint_url:
        kwargs["endpoint_url"] = endpoint_url
    return boto3.client("s3", **kwargs)


def read_checkpoint_state(s3_client, checkpoint_uri):
    """Read incremental checkpoint from S3. Returns None when no checkpoint exists yet."""
    if not checkpoint_uri:
        return None
    bucket, key = parse_s3_uri(checkpoint_uri)
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except ClientError as error:
        if error.response.get("Error", {}).get("Code") in ["NoSuchKey", "404"]:
            return None
        raise
    return json.loads(response["Body"].read().decode("utf-8"))


def write_checkpoint_state(s3_client, checkpoint_uri, checkpoint_state):
    """Persist checkpoint metadata to S3."""
    if not checkpoint_uri:
        return
    bucket, key = parse_s3_uri(checkpoint_uri)
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(checkpoint_state, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


def list_stage_objects(s3_client, bucket, prefix):
    """
    List staged completed-trip objects under a prefix, sorted by (LastModified, Key).

    Each returned row is an S3 object dict from list_objects_v2:
        {
          "Key": "staging/completed_trips/year=2024/month=05/day=25/hour=14/batch_abc.jsonl",
          "LastModified": datetime(2024, 5, 25, 14, 6, tzinfo=timezone.utc),
          "Size": 312,
          ...
        }
    """
    objects = []
    paginator = s3_client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix.rstrip("/") + "/"):
        for obj in page.get("Contents", []):
            if obj.get("Size", 0) > 0:
                objects.append(obj)
    objects.sort(key=lambda o: (o["LastModified"], o["Key"]))
    return objects


def get_relevant_hour_prefixes(staging_prefix, lateness_cap_minutes=60, now=None):
    """
    Return the closed hour prefixes that fall within the lateness window.

    Limits incremental S3 listings to only the relevant hour buckets instead of
    scanning the full staging tree. The currently-open hour is excluded — it has
    only a few minutes of data and will be captured fully on the next scheduled run.

    At 15:05 UTC with cap=60: returns [hour=13, hour=14]. hour=15 is still open.

    Args:
        staging_prefix: Base staging prefix (e.g. "staging/completed_trips")
        lateness_cap_minutes: Lateness window in minutes (default 60)
        now: Override current UTC time for testing
    """
    current_hour = (now or datetime.utcnow()).replace(minute=0, second=0, microsecond=0)
    lookback_hours = int(lateness_cap_minutes // 60) + 1
    prefixes = []
    for offset in range(lookback_hours, 0, -1):  # I'm excluding 0 so I never list the currently-open hour
        h = current_hour - timedelta(hours=offset)
        prefixes.append(
            f"{staging_prefix.rstrip('/')}"
            f"/year={h.year}/month={h.month:02d}/day={h.day:02d}/hour={h.hour:02d}"
        )
    return prefixes


def list_stage_objects_for_prefixes(s3_client, bucket, prefixes):
    """List staged objects across a targeted set of hour prefixes and merge results."""
    objects = []
    for prefix in prefixes:
        objects.extend(list_stage_objects(s3_client, bucket, prefix))
    objects.sort(key=lambda o: (o["LastModified"], o["Key"]))
    return objects


def select_new_stage_objects(object_rows, checkpoint_state):
    """
    Return only the objects newer than the saved checkpoint marker.

    Comparison is a tuple sort on (LastModified ISO string, Key), so objects
    with the same timestamp are ordered lexicographically by key — making the
    checkpoint deterministic even when multiple files land in the same second.

    checkpoint: last_modified="2024-05-25T14:30:00+00:00", last_key="...hour=14/file-b.jsonl"
    file-a.jsonl  modified 14:30:00  → excluded (equal to checkpoint)
    file-b.jsonl  modified 14:30:00  → excluded (equal to checkpoint)
    file-c.jsonl  modified 14:30:00  → included  (key sorts after file-b)
    file-d.jsonl  modified 14:31:00  → included  (newer timestamp)
    """
    if not checkpoint_state:
        return object_rows
    cp_modified = checkpoint_state.get("last_modified", "")
    cp_key = checkpoint_state.get("last_key", "")
    return [
        obj for obj in object_rows
        if (obj["LastModified"].astimezone(timezone.utc).isoformat(), obj["Key"]) > (cp_modified, cp_key)
    ]


def build_checkpoint_state(new_object_rows):
    """
    Build checkpoint metadata from the newest processed staged object.

    Sample output:
        {
          "last_modified": "2024-05-25T14:30:05+00:00",
          "last_key": "staging/completed_trips/.../batch.jsonl",
          "processed_object_count": 42
        }
    """
    if not new_object_rows:
        return None
    latest = new_object_rows[-1]
    return {
        "last_modified": latest["LastModified"].astimezone(timezone.utc).isoformat(),
        "last_key": latest["Key"],
        "processed_object_count": len(new_object_rows),
    }


def extract_hour_partition_prefix(object_key):
    """
    Extract the hour partition prefix from a staged S3 object key.

    "staging/completed_trips/year=2024/month=05/day=25/hour=14/batch.jsonl"
     → "staging/completed_trips/year=2024/month=05/day=25/hour=14"
    """
    parts = object_key.split("/")
    for i, part in enumerate(parts):
        if part.startswith("hour="):
            return "/".join(parts[: i + 1])
    raise ValueError(f"No hour partition found in key: {object_key}")


def collect_affected_partition_prefixes(new_object_rows, lateness_cap_minutes=60, now=None):
    """
    Derive affected hour prefixes from newly arrived staged files.

    When lateness_cap_minutes is None (first run, no checkpoint), all partitions
    are included. Otherwise, partitions whose hour window closed more than
    lateness_cap_minutes ago are dropped.

    The standard run path always passes lateness_cap_minutes=None — lateness is
    already enforced upstream by the targeted S3 listing in get_relevant_hour_prefixes.

    Example with cap=60, now=15:30:
        key=".../hour=14/batch.jsonl"  → hour=14 closed at 15:00, age=30 min ≤ 60  → included
        key=".../hour=12/batch.jsonl"  → hour=12 closed at 13:00, age=150 min > 60 → dropped

    Example with cap=None (first run):
        Both keys above → both included regardless of age
    """
    current_time = now or datetime.utcnow()
    skip_lateness_check = lateness_cap_minutes is None
    allowed_prefixes = set()
    dropped_count = 0

    if skip_lateness_check:
        log.info("No checkpoint — first run. Including all staged partitions.")

    for row in new_object_rows:
        key = row["Key"]
        m = re.search(
            r"year=(?P<year>\d{4})/month=(?P<month>\d{2})/day=(?P<day>\d{2})/hour=(?P<hour>\d{2})",
            key,
        )
        if not m:
            log.warning("Skipping staged key with no hour partition: %s", key)
            continue

        prefix = extract_hour_partition_prefix(key)

        if skip_lateness_check:
            allowed_prefixes.add(prefix)
            continue

        hour_start = datetime(
            int(m.group("year")), int(m.group("month")),
            int(m.group("day")), int(m.group("hour")),
        )
        age_minutes = (current_time - (hour_start + timedelta(hours=1))).total_seconds() / 60

        if age_minutes <= lateness_cap_minutes:
            allowed_prefixes.add(prefix)
        else:
            log.info(
                "Dropping late-arriving staged file: key=%s hour=%s closed %.1f min ago (cap=%d min)",
                key,
                hour_start.strftime("%Y-%m-%d %H"),
                age_minutes,
                lateness_cap_minutes,
            )
            dropped_count += 1

    if dropped_count:
        log.warning(
            "Dropped %d staged file(s) beyond %d-minute lateness cap",
            dropped_count,
            lateness_cap_minutes,
        )

    return sorted(allowed_prefixes)


def read_stage_records_from_s3(s3_client, bucket, partition_prefixes):
    """
    Read all staged JSONL records for the given hour partition prefixes.

    Each line in each JSONL file is a completed-trip event written by lambda_trip_end:
        {
          "trip_id": "c66ce556bc",
          "pickup_location_id": 93,
          "dropoff_location_id": 132,
          "dropoff_datetime": "2024-05-25 14:05:00",
          "fare_amount": 40.1,
          "tip_amount": 0.0,
          "trip_distance": 0.1,
          "trip_status": "completed",
          "data_quality": "ok",
          "record_emitted_at": "2024-05-25T14:06:00+00:00",
          "record_source": "trip_end_update"
        }
    """
    records = []
    for prefix in partition_prefixes:
        for obj in list_stage_objects(s3_client, bucket, prefix):
            body = s3_client.get_object(Bucket=bucket, Key=obj["Key"])["Body"].read().decode("utf-8")
            records.extend(json.loads(line) for line in body.splitlines() if line.strip())
    return records


def deduplicate_completed_trips(frame):
    """
    Filter to valid completed trips and keep the latest record per trip_id.

    Rows dropped:
      - trip_status != "completed"
      - data_quality == "missing_start"  (trip_end arrived before trip_start)
      - pickup_location_id, dropoff_location_id, or dropoff_datetime is null

    When the same trip_id appears more than once (Lambda retry or reprocessing),
    the row with the latest record_emitted_at is kept. Ties break on dropoff_datetime
    then trip_id to ensure deterministic output.
    """
    ranking_window = Window.partitionBy("trip_id").orderBy(
        F.col("record_emitted_at_ts").desc_nulls_last(),
        F.col("dropoff_datetime_ts").desc_nulls_last(),
        F.col("trip_id").asc(),
    )
    return (
        frame.filter(F.col("trip_status") == "completed")
        .filter(F.coalesce(F.col("data_quality"), F.lit("ok")) != "missing_start")
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
        .filter(F.col("row_rank") == 1)
        .drop("row_rank")
    )


def aggregate_completed_trips(records):
    """
    Aggregate staged rows to hourly route metrics. Used by unit tests.

    Returns a sorted list of metric row dicts, one per unique (event_hour, pickup, dropoff).
    """
    rows = list(records)
    if not rows:
        return []

    active = SparkSession.getActiveSession()
    owns_session = active is None
    spark = active or (
        SparkSession.builder
        .appName("etl-project-3-aggregate-completed-trips")
        .master("local[1]")
        .config("spark.sql.shuffle.partitions", "1")
        .getOrCreate()
    )

    try:
        source_frame = spark.createDataFrame([normalize_numeric_values(r) for r in rows])
        metric_frame = transform_completed_trips(source_frame)
        result = (
            metric_frame.select(
                F.date_format("event_hour", "yyyy-MM-dd HH:mm:ss").alias("event_hour"),
                "pickup_location_id",
                "dropoff_location_id",
                "trip_count",
                F.round("fare_sum", 6).alias("fare_sum"),
                F.round("tip_sum", 6).alias("tip_sum"),
                F.round("distance_sum", 6).alias("distance_sum"),
                "year", "month", "day", "hour",
            )
            .orderBy("event_hour", "pickup_location_id", "dropoff_location_id")
            .collect()
        )
        return [row.asDict(recursive=True) for row in result]
    finally:
        if owns_session:
            spark.stop()


def transform_completed_trips(frame):
    """
    Deduplicate and aggregate staged trips into hourly route metrics.

    Output schema (one row per unique event_hour + pickup_location_id + dropoff_location_id):
        event_hour           pickup_location_id  dropoff_location_id  trip_count  fare_sum  tip_sum  distance_sum  year  month  day  hour
        2024-05-25 14:00:00  93                  132                  17          480.13    12.50    31.4          2024  05     25   14
    """
    deduped = deduplicate_completed_trips(frame)
    return (
        deduped
        .withColumn("event_hour", F.date_trunc("hour", F.col("dropoff_datetime_ts")))
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


def transform_top_routes_per_hour(frame, top_limit=3):
    """Rank the top-N routes per hour by trip count and fare, adding derived metrics."""
    metric_frame = transform_completed_trips(frame)
    ranking_window = Window.partitionBy("event_hour").orderBy(
        F.desc("trip_count"), F.desc("fare_sum"), F.desc("tip_sum"),
        F.asc("pickup_location_id"), F.asc("dropoff_location_id"),
    )
    limit = max(int(top_limit), 0)
    return (
        metric_frame
        .withColumn("route_rank_in_hour", F.row_number().over(ranking_window))
        .filter(F.col("route_rank_in_hour") <= limit)
        .withColumn("avg_fare_per_trip", F.round(F.col("fare_sum") / F.col("trip_count"), 6))
        .withColumn("avg_tip_per_trip", F.round(F.col("tip_sum") / F.col("trip_count"), 6))
        .withColumn(
            "tip_to_fare_ratio",
            F.round(
                F.when(F.col("fare_sum") > 0, F.col("tip_sum") / F.col("fare_sum")).otherwise(0.0),
                6,
            ),
        )
    )


def write_output(frame, output_path):
    """Write the aggregated frame to Parquet with dynamic partition overwrite."""
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
    """Run Spark transforms and write affected output partitions. Returns a summary dict."""
    result_frame = transform_completed_trips(source_frame)
    affected_hours = result_frame.select("year", "month", "day", "hour").dropDuplicates().count()
    metric_rows = result_frame.count()
    write_output(result_frame, output_path)

    summary = {"metric_rows": metric_rows, "affected_hours": affected_hours, "top_route_rows": 0}

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
    now=None,
):
    """Run incremental aggregation locally from staged S3 files (Docker / LocalStack)."""
    s3 = get_s3_client(region=region, endpoint_url=endpoint_url)
    checkpoint_state = read_checkpoint_state(s3, checkpoint_uri)

    if checkpoint_state is None:
        log.info("No checkpoint — scanning full staging prefix for first-run backfill.")
        all_objects = list_stage_objects(s3, staging_bucket, staging_prefix)
    else:
        relevant_prefixes = get_relevant_hour_prefixes(staging_prefix, now=now)
        log.info(
            "Checkpoint found — listing %d relevant hour prefix(es): %s",
            len(relevant_prefixes), relevant_prefixes,
        )
        all_objects = list_stage_objects_for_prefixes(s3, staging_bucket, relevant_prefixes)

    new_objects = select_new_stage_objects(all_objects, checkpoint_state)

    if not new_objects:
        log.info("No new staged files since checkpoint. Nothing to aggregate.")
        return {"new_objects": 0, "affected_hours": 0, "metric_rows": 0, "top_route_rows": 0}

    affected_prefixes = collect_affected_partition_prefixes(new_objects, lateness_cap_minutes=None)
    staged_records = read_stage_records_from_s3(s3, staging_bucket, affected_prefixes)

    if not staged_records:
        write_checkpoint_state(s3, checkpoint_uri, build_checkpoint_state(new_objects))
        log.info("New staged files found but no readable completed-trip rows.")
        return {"new_objects": len(new_objects), "affected_hours": 0, "metric_rows": 0, "top_route_rows": 0}

    spark = (
        SparkSession.builder
        .appName("etl-project-3-trip-aggregation-local")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    try:
        source_frame = spark.createDataFrame(
            [normalize_numeric_values(r) for r in staged_records])
        summary = run_incremental_transforms(
            spark=spark,
            source_frame=source_frame,
            output_path=output_path,
            top_routes_output_path=top_routes_output_path,
            top_routes_limit=top_routes_limit,
        )
    finally:
        spark.stop()

    write_checkpoint_state(s3, checkpoint_uri, build_checkpoint_state(new_objects))
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
    """Run incremental aggregation on AWS Glue from staged S3 files."""
    if GlueContext is None:
        raise RuntimeError("awsglue package not available. This path must run on AWS Glue.")

    s3 = get_s3_client(region=region)
    checkpoint_state = read_checkpoint_state(s3, checkpoint_uri)

    if checkpoint_state is None:
        log.info("No checkpoint — scanning full staging prefix for first-run backfill.")
        all_objects = list_stage_objects(s3, staging_bucket, staging_prefix)
    else:
        relevant_prefixes = get_relevant_hour_prefixes(staging_prefix)
        log.info(
            "Checkpoint found — listing %d relevant hour prefix(es): %s",
            len(relevant_prefixes), relevant_prefixes,
        )
        all_objects = list_stage_objects_for_prefixes(s3, staging_bucket, relevant_prefixes)

    new_objects = select_new_stage_objects(all_objects, checkpoint_state)

    spark = SparkSession.builder.getOrCreate()
    glue_context = GlueContext(spark.sparkContext)
    job = Job(glue_context)
    job.init(job_name, {})

    if not new_objects:
        log.info("No new staged files since checkpoint. Glue run will exit cleanly.")
        job.commit()
        return {"new_objects": 0, "affected_hours": 0, "metric_rows": 0, "top_route_rows": 0}

    affected_prefixes = collect_affected_partition_prefixes(new_objects, lateness_cap_minutes=None)
    partition_uris = [f"s3://{staging_bucket}/{p}" for p in affected_prefixes]
    source_frame = spark.read.json(partition_uris)
    summary = run_incremental_transforms(
        spark=spark,
        source_frame=source_frame,
        output_path=output_path,
        top_routes_output_path=top_routes_output_path,
        top_routes_limit=top_routes_limit,
    )

    write_checkpoint_state(s3, checkpoint_uri, build_checkpoint_state(new_objects))
    summary["new_objects"] = len(new_objects)
    log.info("Glue incremental aggregation completed summary=%s", summary)
    job.commit()
    return summary


def parse_cli_args():
    """Parse CLI arguments for local and Glue execution modes."""
    parser = argparse.ArgumentParser(description="Ride-sharing trip aggregation job")
    parser.add_argument("--staging-bucket", default=os.getenv(
        "AGGREGATION_BUCKET", "etl-project-3-analytics-local"))
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
    parser.add_argument(
        "--endpoint-url", default=os.getenv("S3_ENDPOINT_URL") or os.getenv("AWS_ENDPOINT_URL"))
    parser.add_argument(
        "--mode",
        choices=["local_staging", "glue_staging"],
        default="local_staging",
        help="Use local_staging for local simulation and glue_staging for AWS Glue runtime.",
    )
    parser.add_argument(
        "--job-name", default=os.getenv("GLUE_JOB_NAME", "etl-project-3-trip-aggregation"))
    parser.add_argument("--top-routes-output-path", default=os.getenv("TOP_ROUTES_OUTPUT_PATH"))
    parser.add_argument(
        "--top-routes-limit", type=int, default=int(os.getenv("TOP_ROUTES_LIMIT", "3")))
    return parser.parse_args()


def main():
    """Main entry for Glue runtime and local CLI execution."""
    if getResolvedOptions and "JOB_NAME" in os.sys.argv:
        args = getResolvedOptions(
            os.sys.argv,
            ["JOB_NAME", "staging_bucket", "staging_prefix",
             "checkpoint_uri", "output_path", "region"],
        )
        top_routes_output_path = resolve_runtime_option(
            os.sys.argv, "top_routes_output_path", os.getenv("TOP_ROUTES_OUTPUT_PATH"))
        top_routes_limit = to_int_safe(
            resolve_runtime_option(
                os.sys.argv, "top_routes_limit", os.getenv("TOP_ROUTES_LIMIT", "3")),
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
