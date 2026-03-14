"""
Glue and local Spark aggregation for completed trip lifecycle data.

Execution modes:
- local_scan: local development mode using boto3 scan + Spark transform
- glue_connector: AWS Glue mode using DynamoDB Glue connector
"""

import argparse
import logging
import os
from decimal import Decimal

import boto3
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
    Normalize one lifecycle row for Spark DataFrame creation.

    Args:
        row: Raw dictionary from DynamoDB scan or in-memory unit test input

    Returns:
        Dictionary where Decimal values are converted to float

    Sample input:
        {"trip_id": "t-1", "end_fare_amount": Decimal("40.5")}

    Sample output:
        {"trip_id": "t-1", "end_fare_amount": 40.5}
    """
    return {name: (float(value) if isinstance(value, Decimal) else value) for name, value in row.items()}


def resolve_runtime_option(argv, option_name, default_value=None):
    """
    Read optional argument from argv, supporting underscore and dash flag styles.

    Args:
        argv: Full argument list (for example, os.sys.argv)
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


def aggregate_completed_trips(records):
    """
    Aggregate completed lifecycle rows to hourly metrics using native PySpark transforms.

    Args:
        records: Iterable of lifecycle row dictionaries

    Returns:
        Sorted list of hourly metrics grouped by pickup_location_id,
        dropoff_location_id, and event_hour

    Example input:
        [{"trip_status": "completed", "end_dropoff_datetime": "2024-05-25 14:05:00", ...}]

    Example output:
        [{"event_hour": "2024-05-25 14:00:00", "trip_count": 1, ...}]
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
    Transform lifecycle rows into Spark hourly metrics.

    Args:
        frame: Spark DataFrame with lifecycle columns

    Returns:
        Aggregated Spark DataFrame with partition columns
    """
    completed_frame = (
        frame.filter(F.col("trip_status") == F.lit("completed"))
        .filter(F.coalesce(F.col("data_quality"), F.lit("ok")) != F.lit("missing_start"))
        .filter(F.col("start_pickup_location_id").isNotNull())
        .filter(F.col("start_dropoff_location_id").isNotNull())
        .filter(F.col("end_dropoff_datetime").isNotNull())
        .withColumn("event_hour", F.date_trunc("hour", F.to_timestamp("end_dropoff_datetime")))
        .withColumn("fare_amount", F.col("end_fare_amount").cast("double"))
        .withColumn("tip_amount", F.col("end_tip_amount").cast("double"))
        .withColumn("trip_distance", F.col("end_trip_distance").cast("double"))
    )

    metric_frame = (
        completed_frame.groupBy(
            "event_hour",
            F.col("start_pickup_location_id").cast("int").alias("pickup_location_id"),
            F.col("start_dropoff_location_id").cast("int").alias("dropoff_location_id"),
        )
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
        frame: Spark DataFrame with lifecycle columns
        top_limit: Number of top ranked routes to keep per hour

    Returns:
        Spark DataFrame with ranking and route quality metrics

    Sample output columns:
        event_hour, pickup_location_id, dropoff_location_id, trip_count,
        fare_sum, tip_sum, distance_sum, route_rank_in_hour, avg_fare_per_trip,
        avg_tip_per_trip, tip_to_fare_ratio, year, month, day, hour
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


def read_dynamo_via_glue(glue_context, table_name, region):
    """
    Read DynamoDB table through Glue DynamicFrame connector.

    Args:
        glue_context: GlueContext object
        table_name: DynamoDB table name
        region: AWS region

    Returns:
        Spark DataFrame
    """
    dynamic_frame = glue_context.create_dynamic_frame.from_options(
        connection_type="dynamodb",
        connection_options={
            "dynamodb.input.tableName": table_name,
            "dynamodb.region": region,
            "dynamodb.splits": "50",
            "dynamodb.throughput.read.percent": "0.5",
        },
    )
    return dynamic_frame.toDF()


def read_dynamo_via_scan(spark, table_name, region, endpoint_url=None):
    """
    Read DynamoDB table with boto3 scan for local mode.

    Args:
        spark: Spark session
        table_name: DynamoDB table name
        region: AWS region
        endpoint_url: Optional local endpoint URL

    Returns:
        Spark DataFrame
    """
    client_options = {"region_name": region}
    if endpoint_url:
        client_options["endpoint_url"] = endpoint_url

    table = boto3.resource("dynamodb", **client_options).Table(table_name)
    response = table.scan()
    items = response.get("Items", [])

    while "LastEvaluatedKey" in response:
        response = table.scan(ExclusiveStartKey=response["LastEvaluatedKey"])
        items.extend(response.get("Items", []))

    normalized_items = []
    for row in items:
        normalized_items.append(normalize_numeric_values(row))

    return spark.createDataFrame(normalized_items)


def write_output(frame, output_path):
    """
    Write hourly metrics to parquet with partition overwrite mode.

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


def run_local_scan_aggregation(
    table_name,
    output_path,
    region,
    endpoint_url=None,
    top_routes_output_path=None,
    top_routes_limit=3,
):
    """
    Run local scan mode aggregation end-to-end.

    Args:
        table_name: Lifecycle table name
        output_path: Local/S3 parquet output path
        region: AWS region
        endpoint_url: Local Dynamo endpoint
    """
    spark = (
        SparkSession.builder.appName("etl-project-3-trip-aggregation-local")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )

    try:
        source_frame = read_dynamo_via_scan(
            spark=spark,
            table_name=table_name,
            region=region,
            endpoint_url=endpoint_url,
        )
        result_frame = transform_completed_trips(source_frame)
        write_output(result_frame, output_path)
        log.info("Local aggregation completed -> %s", output_path)

        if top_routes_output_path:
            top_routes_frame = transform_top_routes_per_hour(source_frame, top_limit=top_routes_limit)
            write_output(top_routes_frame, top_routes_output_path)
            log.info(
                "Local top-routes aggregation completed -> %s (top_limit=%s)",
                top_routes_output_path,
                top_routes_limit,
            )
    finally:
        spark.stop()


def run_glue_aggregation(
    table_name,
    output_path,
    region,
    job_name,
    top_routes_output_path=None,
    top_routes_limit=3,
):
    """
    Run Glue connector mode aggregation end-to-end.

    Args:
        table_name: Lifecycle table name
        output_path: S3 parquet output path
        region: AWS region
        job_name: Glue job name
    """
    if GlueContext is None:
        raise RuntimeError("awsglue package not available. This path must run on AWS Glue.")

    spark = SparkSession.builder.getOrCreate()
    glue_context = GlueContext(spark.sparkContext)
    job = Job(glue_context)
    job.init(job_name, {})

    source_frame = read_dynamo_via_glue(glue_context, table_name=table_name, region=region)
    result_frame = transform_completed_trips(source_frame)
    write_output(result_frame, output_path)
    log.info("Glue aggregation completed -> %s", output_path)

    if top_routes_output_path:
        top_routes_frame = transform_top_routes_per_hour(source_frame, top_limit=top_routes_limit)
        write_output(top_routes_frame, top_routes_output_path)
        log.info(
            "Glue top-routes aggregation completed -> %s (top_limit=%s)",
            top_routes_output_path,
            top_routes_limit,
        )

    job.commit()


def parse_cli_args():
    """
    Parse CLI arguments for local and Glue execution modes.

    Returns:
        argparse namespace
    """
    parser = argparse.ArgumentParser(description="ETL-Project-3 trip aggregation job")
    parser.add_argument("--table-name", default=os.getenv("TRIP_LIFECYCLE_TABLE", "trip_lifecycle"))
    parser.add_argument("--output-path", required=True)
    parser.add_argument("--region", default=os.getenv("AWS_REGION", "ap-south-2"))
    parser.add_argument("--endpoint-url", default=os.getenv("DYNAMODB_ENDPOINT_URL") or os.getenv("AWS_ENDPOINT_URL"))
    parser.add_argument(
        "--mode",
        choices=["local_scan", "glue_connector"],
        default="local_scan",
        help="Use local_scan for local simulation and glue_connector for AWS Glue runtime.",
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
            ["JOB_NAME", "table_name", "output_path", "region"],
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
        run_glue_aggregation(
            table_name=args["table_name"],
            output_path=args["output_path"],
            region=args["region"],
            job_name=args["JOB_NAME"],
            top_routes_output_path=top_routes_output_path,
            top_routes_limit=top_routes_limit,
        )
        return

    args = parse_cli_args()
    if args.mode == "glue_connector":
        run_glue_aggregation(
            table_name=args.table_name,
            output_path=args.output_path,
            region=args.region,
            job_name=args.job_name,
            top_routes_output_path=args.top_routes_output_path,
            top_routes_limit=args.top_routes_limit,
        )
    else:
        run_local_scan_aggregation(
            table_name=args.table_name,
            output_path=args.output_path,
            region=args.region,
            endpoint_url=args.endpoint_url,
            top_routes_output_path=args.top_routes_output_path,
            top_routes_limit=args.top_routes_limit,
        )


if __name__ == "__main__":
    main()
