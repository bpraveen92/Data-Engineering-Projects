#!/usr/bin/env python3
"""
Spark Aggregator for ETL-Project-2

Reads streaming events from Kinesis, enriches with dimension tables (songs, users),
applies time-windowed aggregations with watermarking, and writes results to S3/MinIO.
"""

import argparse
import logging
import os
import sys

try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    pass  # dotenv not available in AWS Glue runtime — env vars come from job parameters

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    approx_count_distinct,
    broadcast,
    col,
    count,
    from_json,
    max as spark_max,
    to_timestamp,
    window,
)
from pyspark.sql.types import StructField, StructType, StringType

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Target output Parquet file size for compaction (used in write_to_s3)
TARGET_FILE_SIZE_BYTES = 5 * 1024 * 1024  # 5 MB


def create_spark_session(use_localstack=False):
    """
    Create and configure Spark session.

    Args:
        use_localstack: If True, configures S3A→MinIO and Kinesis→LocalStack endpoints.

    Returns:
        Configured SparkSession.
    """
    builder = (
        SparkSession.builder
        .appName("MusicStreamingAggregator")
        # Reduce shuffle output partitions from the default of 200 to avoid
        # hundreds of tiny part files per micro-batch with small local data.
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.streaming.schemaInference", "true")
    )

    if use_localstack:
        # --- S3A / MinIO ---
        s3_endpoint = os.getenv(
            'S3_ENDPOINT', 'http://etl-project-2-minio:9000')
        s3_access_key = os.getenv('S3_ACCESS_KEY', 'minioadmin')
        s3_secret_key = os.getenv('S3_SECRET_KEY', 'minioadmin')

        builder = (
            builder
            .config("spark.hadoop.fs.s3a.endpoint", s3_endpoint)
            .config("spark.hadoop.fs.s3a.access.key", s3_access_key)
            .config("spark.hadoop.fs.s3a.secret.key", s3_secret_key)
            .config("spark.hadoop.fs.s3a.path.style.access", "true")
            .config("spark.hadoop.fs.s3a.endpoint.region", "us-east-1")
        )

        # --- JVM AWS credentials ---
        # Container env vars are not forwarded to the JVM; inject them as
        # system properties so the Kinesis connector's AWS SDK picks them up.
        aws_access_key = os.getenv('AWS_ACCESS_KEY_ID', 'test')
        aws_secret_key = os.getenv('AWS_SECRET_ACCESS_KEY', 'test')
        jvm_creds = (
            f"-Daws.accessKeyId={aws_access_key} "
            f"-Daws.secretAccessKey={aws_secret_key}"
        )

        builder = (
            builder
            .config("spark.driver.extraJavaOptions", jvm_creds)
            .config("spark.executor.extraJavaOptions", jvm_creds)
        )

        # --- Kinesis endpoint (LocalStack) ---
        # Set all three keys — different connector layers read from different ones.
        kinesis_endpoint = os.getenv(
            'KINESIS_ENDPOINT', 'http://etl-project-2-localstack:4566')

        builder = (
            builder
            .config("spark.kinesis.endpointUrl", kinesis_endpoint)
            .config("kinesis.endpointUrl", kinesis_endpoint)
            .config("spark.hadoop.kinesis.endpointUrl", kinesis_endpoint)
        )

    return builder.getOrCreate()


def get_kinesis_schema():
    """Return StructType schema for incoming Kinesis JSON messages."""
    return StructType([
        StructField("user_id", StringType(), True),
        StructField("track_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("source", StringType(), True)
    ])


def read_kinesis(spark, kinesis_stream, region='ap-south-2', use_localstack=False, metadata_suffix='default', checkpoint_path=None):
    """
    Read streaming data from Kinesis.

    Args:
        spark: SparkSession object.
        kinesis_stream: Kinesis stream name.
        region: AWS region.
        use_localstack: If True, override endpoints to use LocalStack.
        metadata_suffix: Unique suffix for kinesis.metadataPath — each concurrent
            writeStream query must have its own metadata directory to avoid
            "Unable to fetch committed metadata" errors on restart.
        checkpoint_path: Base checkpoint path. Used to derive the metadataPath for
            local runs. Defaults to production S3 path.

    Returns:
        Streaming DataFrame of raw Kinesis records.
    """
    logger.info(
        f"Reading from Kinesis stream: {kinesis_stream} (metadata_suffix={metadata_suffix})")

    if use_localstack and checkpoint_path:
        metadata_path = f"{checkpoint_path}/kinesis-metadata-{metadata_suffix}"
    else:
        metadata_path = f"s3a://pravbala-data-engineering-projects/Project-2/checkpoints/kinesis-metadata-{metadata_suffix}"

    options = {
        "kinesis.streamName": kinesis_stream,
        "kinesis.initialPosition": "TRIM_HORIZON",
        # Both keys required: connector layer uses kinesis.region, AWS SDK inside
        # the connector uses kinesis.kinesisRegion (see TROUBLESHOOTING.md L2).
        "kinesis.region": region,
        "kinesis.kinesisRegion": region,
        # Connector v1.0.0 does not auto-derive the endpoint — must be set
        # explicitly even for real AWS (see TROUBLESHOOTING.md G2).
        "kinesis.endpointUrl": f"https://kinesis.{region}.amazonaws.com",
        # s3a:// path required so the HDFSMetadataCommitter uses the configured
        # filesystem; persists metadata across restarts (see TROUBLESHOOTING.md G3-G5).
        # Each concurrent query must have a unique suffix to avoid metadata conflicts.
        "kinesis.metadataPath": metadata_path,
    }

    if use_localstack:
        endpoint = os.getenv("KINESIS_ENDPOINT")
        if not endpoint:
            raise ValueError(
                "use_localstack=True but KINESIS_ENDPOINT env var is not set."
            )

        # Ensure endpoint has a protocol prefix
        if not endpoint.startswith("http"):
            endpoint = f"http://{endpoint}"

        logger.info(f"Using LocalStack Kinesis endpoint: {endpoint}")

        options.update({
            "kinesis.endpointUrl": endpoint,
            # Disable SSL checks — LocalStack serves plain HTTP.
            "kinesis.verifyCertificate": "false",
            "kinesis.allowUnauthorizedSsl": "true",
        })

    df = (
        spark.readStream
        .format("org.apache.spark.sql.connector.kinesis.KinesisV2TableProvider")
        .options(**options)
        .load()
    )

    return df


def parse_events(df):
    """
    Parse JSON payloads from raw Kinesis records.

    Args:
        df: Streaming DataFrame with raw Kinesis records.

    Returns:
        DataFrame with parsed event columns and an event_timestamp column.
    """
    schema = get_kinesis_schema()
    parsed = df.select(
        from_json(col("data").cast("STRING"), schema).alias("payload")
    ).select("payload.*").withColumn("event_timestamp", to_timestamp(col("timestamp")))
    return parsed


def load_songs(spark, path):
    """Load and cache the songs dimension CSV."""
    logger.info(f"Loading songs from: {path}")
    df = spark.read.csv(path, header=True, inferSchema=True).cache()
    logger.info(f"Loaded {df.count()} songs")
    return df


def load_users(spark, path):
    """Load and cache the users dimension CSV."""
    logger.info(f"Loading users from: {path}")
    df = spark.read.csv(path, header=True, inferSchema=True).cache()
    logger.info(f"Loaded {df.count()} users")
    return df


def enrich_events(events, songs, users):
    """Broadcast-join events with songs and users dimension tables."""
    logger.info("Enriching events with dimensions...")
    enriched = events.join(broadcast(songs), on="track_id", how="left") \
                     .join(broadcast(users), on="user_id", how="left")
    return enriched


def compute_hourly_streams(df, window_minutes=2, watermark_minutes=0.5):
    """
    Compute per-user/track/country stream counts within a sliding window.

    Args:
        df: Enriched streaming DataFrame.
        window_minutes: Window duration in minutes.
        watermark_minutes: Late-data tolerance in minutes.

    Returns:
        Aggregated DataFrame with stream_count per window/user/track/country.
    """
    logger.info("Computing hourly streams...")
    window_duration = f"{int(window_minutes * 60)} seconds"
    watermark_duration = f"{int(watermark_minutes * 60)} seconds"

    return (
        df
        .withWatermark("event_timestamp", watermark_duration)
        .groupBy(
            window(col("event_timestamp"), window_duration),
            col("user_id"),
            col("track_id"),
            col("user_country"),
        )
        .agg(
            count("*").alias("stream_count"),
            spark_max("event_timestamp").alias("last_event_time"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "user_id",
            "track_id",
            "user_country",
            "stream_count",
            "last_event_time",
        )
    )


def compute_top_tracks(df, window_minutes=2, watermark_minutes=0.5):
    """
    Compute total streams and unique listeners per track within a window.

    Args:
        df: Enriched streaming DataFrame.
        window_minutes: Window duration in minutes.
        watermark_minutes: Late-data tolerance in minutes.

    Returns:
        DataFrame with total_streams and unique_listeners per window/track.
    """
    logger.info("Computing top tracks...")
    window_duration = f"{int(window_minutes * 60)} seconds"
    watermark_duration = f"{int(watermark_minutes * 60)} seconds"

    return (
        df
        .withWatermark("event_timestamp", watermark_duration)
        .groupBy(
            window(col("event_timestamp"), window_duration),
            col("track_id"),
            col("track_name"),
            col("artists"),
        )
        .agg(
            count("*").alias("total_streams"),
            approx_count_distinct("user_id").alias("unique_listeners"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "track_id",
            "track_name",
            "artists",
            "total_streams",
            "unique_listeners",
        )
    )


def compute_country_metrics(df, window_minutes=2, watermark_minutes=0.5):
    """
    Compute per-country stream totals, unique users/tracks and avg track popularity.

    Args:
        df: Enriched streaming DataFrame.
        window_minutes: Window duration in minutes.
        watermark_minutes: Late-data tolerance in minutes.

    Returns:
        DataFrame with country-level aggregates per window.
    """
    logger.info("Computing country metrics...")
    window_duration = f"{int(window_minutes * 60)} seconds"
    watermark_duration = f"{int(watermark_minutes * 60)} seconds"

    return (
        df
        .withWatermark("event_timestamp", watermark_duration)
        .groupBy(
            window(col("event_timestamp"), window_duration),
            col("user_country"),
        )
        .agg(
            count("*").alias("total_streams"),
            approx_count_distinct("user_id").alias("unique_users"),
            approx_count_distinct("track_id").alias("unique_tracks"),
            avg("popularity").alias("avg_track_popularity"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            "user_country",
            "total_streams",
            "unique_users",
            "unique_tracks",
            "avg_track_popularity",
        )
    )


def write_to_s3(df, output_path, table_name, checkpoint_path='/tmp/spark_checkpoints'):
    """
    Write a streaming aggregation to S3/MinIO using foreachBatch for file compaction.

    Each micro-batch is coalesced to ~5 MB files before writing, preventing
    the default behaviour of emitting hundreds of tiny part-files per batch.

    Args:
        df: Streaming DataFrame to write.
        output_path: Base S3 path (e.g. s3a://bucket/aggregations).
        table_name: Sub-directory name for this table's output.
        checkpoint_path: Checkpoint root directory for recovery.

    Returns:
        StreamingQuery object.
    """
    # TARGET_FILE_SIZE_BYTES is defined at module level (5 MB).

    def write_batch(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            logger.info(f"[{table_name}] Batch {batch_id} is empty, skipping.")
            return

        # Estimate total batch size: row count * avg row size (approx 200 bytes)
        row_count = batch_df.count()
        estimated_bytes = row_count * 200
        # Calculate number of output files to target ~5MB each (minimum 1)
        num_files = max(1, int(estimated_bytes / TARGET_FILE_SIZE_BYTES))

        logger.info(
            f"[{table_name}] Batch {batch_id}: {row_count} rows, "
            f"~{estimated_bytes / 1024:.1f}KB estimated → coalescing to {num_files} file(s)"
        )

        batch_df.coalesce(num_files) \
            .write \
            .mode("append") \
            .partitionBy("window_start") \
            .parquet(f"{output_path}/{table_name}")

    logger.info(f"Writing {table_name} to {output_path}/{table_name}...")
    try:
        query = df.writeStream \
            .outputMode("append") \
            .option("checkpointLocation", f"{checkpoint_path}/{table_name}") \
            .foreachBatch(write_batch) \
            .start()
        logger.info(f"Query started for {table_name}: {query.id}")
        return query
    except Exception as e:
        logger.error(
            f"Failed to start query for {table_name}: {e}", exc_info=True)
        raise


def run_pipeline(spark, kinesis_stream, songs_path, users_path, output_path, region='ap-south-2', window_minutes=2, watermark_minutes=0.5, checkpoint_path='/tmp/spark_checkpoints', use_localstack=False):
    """
    Execute the end-to-end streaming pipeline.

    Loads dimension tables, reads from Kinesis, enriches events, computes
    three aggregations (hourly streams, top tracks, country metrics) and
    writes each to S3/MinIO as compacted Parquet.

    Args:
        spark: SparkSession object.
        kinesis_stream: Kinesis stream name.
        songs_path: Path to songs.csv.
        users_path: Path to users.csv.
        output_path: Base S3 path for output tables.
        region: AWS region.
        window_minutes: Aggregation window size in minutes.
        watermark_minutes: Late-data watermark in minutes.
        checkpoint_path: Checkpoint root directory.
        use_localstack: If True, routes Kinesis reads to LocalStack.
    """
    try:
        logger.info("Starting streaming aggregation pipeline...")

        songs = load_songs(spark, songs_path)
        users = load_users(spark, users_path)

        # Each aggregation query gets its own Kinesis read with a unique
        # kinesis.metadataPath suffix. Sharing a single raw DataFrame across
        # multiple writeStream.start() calls causes each query's
        # KinesisV2MicrobatchStream to compete for the same metadata directory,
        # producing "Unable to fetch committed metadata from previous batch id 0".
        def make_enriched(suffix):
            raw = read_kinesis(spark, kinesis_stream, region,
                               use_localstack, metadata_suffix=suffix,
                               checkpoint_path=checkpoint_path)
            events = parse_events(raw)
            return enrich_events(events, songs, users)

        hourly = compute_hourly_streams(
            make_enriched("hourly"), window_minutes, watermark_minutes)
        top_tracks = compute_top_tracks(
            make_enriched("top_tracks"), window_minutes, watermark_minutes)
        country = compute_country_metrics(
            make_enriched("country"), window_minutes, watermark_minutes)

        queries = [
            write_to_s3(hourly, output_path,
                        "hourly_streams", checkpoint_path),
            write_to_s3(top_tracks, output_path,
                        "top_tracks_hourly", checkpoint_path),
            write_to_s3(country, output_path,
                        "country_metrics_hourly", checkpoint_path),
        ]
        logger.info(
            f"All {len(queries)} streaming queries started, awaiting termination...")

        for query in queries:
            query.awaitTermination()

    except Exception as e:
        logger.error(f"Pipeline failed: {e}", exc_info=True)
        raise


def main():
    """CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Aggregate music streaming events in real-time')
    parser.add_argument('--kinesis-stream', required=True)
    parser.add_argument('--songs-path', required=True)
    parser.add_argument('--users-path', required=True)
    parser.add_argument('--output-path', required=True)
    parser.add_argument('--region', default='ap-south-2')
    parser.add_argument('--window-minutes', type=float, default=5)
    parser.add_argument('--watermark-minutes', type=float, default=1)
    parser.add_argument('--checkpoint-path', default='/tmp/spark_checkpoints')
    parser.add_argument('--local', action='store_true', help='Use LocalStack')

    # parse_known_args() instead of parse_args() so that Glue's internally
    # injected arguments (--JOB_NAME, --job-bookmark-option, etc.) are silently
    # ignored rather than causing argparse to exit with SystemExit: 2.
    args, unknown = parser.parse_known_args()
    if unknown:
        logger.warning(
            f"Ignoring unrecognized arguments (likely Glue internals): {unknown}")

    try:
        spark = create_spark_session(args.local)
        run_pipeline(
            spark=spark,
            kinesis_stream=args.kinesis_stream,
            songs_path=args.songs_path,
            users_path=args.users_path,
            output_path=args.output_path,
            region=args.region,
            window_minutes=args.window_minutes,
            watermark_minutes=args.watermark_minutes,
            checkpoint_path=args.checkpoint_path,
            use_localstack=args.local
        )
    except Exception as e:
        logger.error(f"Failed: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
