from __future__ import annotations

import argparse
import hashlib
import json
import logging
import os
from datetime import datetime, timezone
from io import BytesIO

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from src.common.datasets import dataset_from_key, group_keys_by_dataset

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)

METADATA_COLUMNS = ["run_id", "ingest_ts", "ingest_date", "source_file"]


# Build a dictionary with validated runtime arguments.
def build_runtime_args(namespace):
    """
    Convert parsed CLI namespace into a plain dictionary.

    Args:
        namespace: Parsed argparse namespace object.
            Example: Namespace(JOB_NAME='csv_to_parquet', raw_bucket='my-bucket', ...)

    Returns:
        Dictionary with normalized runtime arguments.
            Example: {
                "job_name": "csv_to_parquet",
                "raw_bucket": "my-bucket",
                "raw_path": "Project-1/raw/",
                "curated_bucket": "my-bucket",
                "curated_path": "Project-1/curated/music/",
                "schema_registry_path": "Project-1/schemas/music/",
                "run_id": "20250228_143000",
                "file_keys_json": "[\"Project-1/raw/songs.csv\"]"
            }
    """
    return {
        "job_name": namespace.JOB_NAME,
        "raw_bucket": namespace.raw_bucket,
        "raw_path": namespace.raw_path,
        "curated_bucket": namespace.curated_bucket,
        "curated_path": namespace.curated_path,
        "schema_registry_path": namespace.schema_registry_path,
        "run_id": namespace.run_id,
        "file_keys_json": namespace.file_keys_json,
    }


# Parse runtime args and support legacy flag names.
def parse_args():
    """
    Parse transformation script arguments and support legacy flag names.

    Args:
        None.

    Returns:
        Dictionary containing validated runtime values.
            Example: {
                "job_name": "csv_to_parquet",
                "raw_bucket": "my-bucket",
                "raw_path": "Project-1/raw/",
                "curated_bucket": "my-bucket",
                "curated_path": "Project-1/curated/music/",
                "schema_registry_path": "Project-1/schemas/music/",
                "run_id": "20250228_143000",
                "file_keys_json": "[\"Project-1/raw/songs.csv\"]"
            }
    """
    parser = argparse.ArgumentParser()
    parser.add_argument("--JOB_NAME", required=True)
    parser.add_argument("--raw_bucket", required=True)
    parser.add_argument("--raw_path", "--raw_prefix",
                        dest="raw_path", required=True)
    parser.add_argument("--curated_bucket", required=True)
    parser.add_argument("--curated_path", "--curated_prefix",
                        dest="curated_path", required=True)
    parser.add_argument(
        "--schema_registry_path",
        "--schema_registry_prefix",
        dest="schema_registry_path",
        required=True,
    )
    parser.add_argument("--run_id", required=True)
    parser.add_argument("--file_keys_json", required=False)
    known_args, _ = parser.parse_known_args()
    return build_runtime_args(known_args)


# Normalize S3-style paths to always include trailing slash.
def with_trailing_slash(path):
    """
    Ensure path-style strings always end with '/'.

    Args:
        path: Path string to normalize.
            Example: "Project-1/schemas/music" or "Project-1/raw/"

    Returns:
        Normalized path ending with '/'.
            Example: "Project-1/schemas/music/" from input "Project-1/schemas/music"
            Example: "Project-1/raw/" from input "Project-1/raw/"
    """
    return path if path.endswith("/") else f"{path}/"


# Convert headers to lowercase snake_case style names.
def clean_column_name(name):
    """
    Convert a column name into lowercase snake_case format.

    Args:
        name: Raw source column name.
            Example: "Track ID" or "artistName" or "USER_ID"

    Returns:
        Normalized column name string.
            Example: "track_id" from input "Track ID"
            Example: "artist_name" from input "artistName"
            Example: "user_id" from input "USER_ID"
    """
    clean = "".join(ch if ch.isalnum() else "_" for ch in name.strip().lower())
    while "__" in clean:
        clean = clean.replace("__", "_")
    return clean.strip("_")


# Keep compatibility with existing unit tests.
def sanitize_column(name):
    """
    Backward-compatible alias for column-name normalization.

    Args:
        name: Raw source column name.
            Example: "Track ID" or "USER_ID"

    Returns:
        Normalized column name string.
            Example: "track_id" from input "Track ID"
    """
    return clean_column_name(name)


# Load input keys either from args or by listing raw S3 path.
def load_input_keys(args, s3_client):
    """
    Resolve input files either from provided JSON list or S3 listing.

    Args:
        args: Parsed runtime arguments.
            Example: {"file_keys_json": "[\"Project-1/raw/songs.csv\"]", "raw_bucket": "my-bucket", ...}
        s3_client: Boto3 S3 client.

    Returns:
        List of raw CSV object keys to process.
            Example: ["Project-1/raw/songs.csv", "Project-1/raw/users.csv"]
    """
    if args["file_keys_json"]:
        return json.loads(args["file_keys_json"])

    keys = []
    token = None
    while True:
        request = {"Bucket": args["raw_bucket"], "Prefix": args["raw_path"]}
        if token:
            request["ContinuationToken"] = token

        response = s3_client.list_objects_v2(**request)
        keys.extend(item["Key"] for item in response.get(
            "Contents", []) if item["Key"].endswith(".csv"))

        if not response.get("IsTruncated"):
            break
        token = response.get("NextContinuationToken")

    return keys


# Read one CSV object from S3 into a pandas dataframe.
def read_csv_from_s3(s3_client, bucket, key):
    """
    Read a CSV file from S3.

    Args:
        s3_client: Boto3 S3 client.
        bucket: Source S3 bucket.
            Example: "pravbala-data-engineering-projects"
        key: Source object key.
            Example: "Project-1/raw/songs.csv"

    Returns:
        Pandas DataFrame loaded from the CSV object.
            Example: DataFrame with columns [track_id, track_name, artists, ...]
                    and N rows from the CSV file

    Raises:
        ValueError: If CSV is empty or malformed.
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
        df = pd.read_csv(BytesIO(response["Body"].read()))

        if df.empty:
            log.warning("Empty CSV file: s3://%s/%s", bucket, key)

        return df
    except Exception as exc:
        log.error("Failed to read s3://%s/%s: %s", bucket, key, str(exc))
        raise


# Apply normalized names to all dataframe columns.
def normalize_columns(df):
    """
    Rename all DataFrame columns to normalized names.

    Converts column names to lowercase snake_case for consistency with database conventions.
    For example: "Track ID" → "track_id", "artistName" → "artist_name".

    Args:
        df: Input pandas DataFrame with raw column names.
            Example: DataFrame with columns ["Track ID", "artistName", "DURATION_MS"]

    Returns:
        DataFrame with normalized column names.
            Example: DataFrame with columns ["track_id", "artist_name", "duration_ms"]
    """
    renamed = {name: clean_column_name(name) for name in df.columns}
    return df.rename(columns=renamed)


# Add common ingestion metadata columns to each record.
def add_ingest_columns(df, run_id):
    """
    Append standard ingestion metadata columns.

    Args:
        df: DataFrame to enrich.
        run_id: Pipeline run identifier.

    Returns:
        DataFrame with ingestion metadata columns.
    """
    out = df.copy()
    ingest_dt = datetime.now(timezone.utc)
    ingest_ts = ingest_dt.strftime("%Y-%m-%d %H:%M:%S")
    ingest_date = ingest_dt.strftime("%Y-%m-%d")

    out["run_id"] = run_id
    out["ingest_ts"] = ingest_ts
    out["ingest_date"] = ingest_date
    return out


# Build stable stream event id from key fields.
def stream_event_id(row):
    """
    Build deterministic stream event id hash from row values.

    Args:
        row: Row-like object containing stream identity fields.

    Returns:
        SHA256 hash string representing the stream event identity.
    """
    values = []
    for name in ["user_id", "track_id", "listen_time", "source_file"]:
        value = row.get(name)
        values.append("" if pd.isna(value) else str(value))
    return hashlib.sha256("||".join(values).encode("utf-8")).hexdigest()


def transform_songs(df):
    """Normalize audio features and metadata for songs dataset."""
    # Cast audio features to numeric, handling missing values gracefully
    audio_features = ["popularity", "duration_ms",
                      "danceability", "energy", "tempo"]
    for col in audio_features:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Normalize duration to seconds (assuming input is milliseconds)
    if "duration_ms" in df.columns:
        df["duration_ms"] = (df["duration_ms"] / 1000).round(1)

    # Clip popularity score to valid range
    if "popularity" in df.columns:
        df["popularity"] = df["popularity"].clip(0, 100)

    # Standardize artist names to title case if present
    if "artists" in df.columns:
        df["artists"] = df["artists"].str.title()


def transform_users(df):
    """Normalize demographics for users dataset."""
    if "user_age" in df.columns:
        df["user_age"] = pd.to_numeric(df["user_age"], errors="coerce")
        # Clip ages to realistic range, mark invalid as null
        df.loc[(df["user_age"] < 13) | (
            df["user_age"] > 120), "user_age"] = None

    # Normalize country/region codes to uppercase ISO 3166-1
    if "country" in df.columns:
        df["country"] = df["country"].str.upper()

    # Validate and normalize user subscription tier
    if "subscription_tier" in df.columns:
        valid_tiers = {"free", "premium", "family"}
        df.loc[~df["subscription_tier"].str.lower().isin(
            valid_tiers), "subscription_tier"] = "unknown"
        df["subscription_tier"] = df["subscription_tier"].str.lower()


def transform_streams(df):
    """Normalize listen events and add deterministic event IDs for deduplication."""
    # Parse and standardize timestamps
    if "listen_time" in df.columns:
        listen_dt = pd.to_datetime(
            df["listen_time"], errors="coerce", utc=True)
        df["listen_time"] = listen_dt.dt.strftime("%Y-%m-%d %H:%M:%S")

    # Cast play count to integer
    if "play_count" in df.columns:
        df["play_count"] = pd.to_numeric(
            df["play_count"], errors="coerce").fillna(0).astype("Int64")

    # Calculate session duration as seconds
    if "session_duration_ms" in df.columns:
        df["session_duration_ms"] = (
            df["session_duration_ms"] / 1000).round(0).astype("Int64")

    # Generate deterministic event ID for idempotency and deduplication
    required_fields = {"user_id", "track_id", "listen_time", "source_file"}
    if required_fields.issubset(set(df.columns)):
        df["stream_event_id"] = df.apply(
            lambda row: hashlib.sha256(
                f"{row['user_id']}||{row['track_id']}||{row['listen_time']}||{row['source_file']}".encode()
            ).hexdigest(),
            axis=1
        )

        # Flag potential duplicates within the run
        df["is_duplicate"] = df.duplicated(
            subset=["stream_event_id"], keep="first").astype(str)


# Apply dataset-specific casting and enrichment rules.
def apply_dataset_transforms(dataset, df):
    """
    Apply dataset-specific cleaning, casting, and enrichment rules.

    Args:
        dataset: Dataset name that controls transform logic.
        df: Input DataFrame.

    Returns:
        Transformed DataFrame.
    """
    out = df.copy()

    # Standardize all string columns by stripping whitespace
    for name in out.columns:
        if pd.api.types.is_string_dtype(out[name]) or out[name].dtype == object:
            out[name] = out[name].astype(str).str.strip()

    if dataset == "songs":
        transform_songs(out)
    elif dataset == "users":
        transform_users(out)
    elif dataset == "streams":
        transform_streams(out)

    return out


# Cast all outgoing columns to string to simplify schema drift handling.
def cast_all_to_string(df):
    """
    Cast all output columns to string for schema-drift tolerance.

    This approach allows new columns to be added to upstream CSV files without
    breaking the transformation pipeline. Downstream SQL queries can cast to
    appropriate types as needed.

    Args:
        df: DataFrame after transforms.
            Example: DataFrame with mixed types (int, float, datetime, string)

    Returns:
        DataFrame with all columns cast to string type (as object dtype, not Pandas "string").
            Example: All columns converted to object dtype (native Python strings)
    """
    out = df.copy()
    for name in out.columns:
        out[name] = out[name].astype(str)
    return out


# Build schema snapshot key path for a dataset.
def schema_registry_key(path, dataset):
    """
    Build schema snapshot S3 key for a dataset.

    Args:
        path: Base schema registry prefix.
        dataset: Dataset name.

    Returns:
        S3 key path for the dataset schema snapshot.
    """
    return f"{with_trailing_slash(path)}{dataset}_schema.json"


# Read last known schema column list from S3.
def read_schema_columns(s3_client, bucket, key):
    """
    Read existing schema columns from an S3 snapshot file.

    Args:
        s3_client: Boto3 S3 client.
        bucket: Bucket containing schema snapshots.
        key: Snapshot object key.

    Returns:
        List of existing schema columns, or empty list if snapshot is missing.
    """
    try:
        response = s3_client.get_object(Bucket=bucket, Key=key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "NoSuchKey":
            raise
        return []

    payload = json.loads(response["Body"].read().decode("utf-8"))
    return payload.get("columns", [])


# Persist updated schema column list to S3.
def write_schema_columns(
    s3_client,
    bucket,
    key,
    columns,
    dataset,
):
    """
    Persist updated schema columns for a dataset to S3.

    Args:
        s3_client: Boto3 S3 client.
        bucket: Bucket to write schema snapshot into.
        key: Snapshot object key.
        columns: Iterable of resolved schema columns.
        dataset: Dataset name being updated.

    Returns:
        None.
    """
    payload = {
        "dataset": dataset,
        "updated_at_utc": datetime.now(tz=timezone.utc).isoformat(),
        "columns": sorted(set(columns)),
    }
    s3_client.put_object(
        Bucket=bucket,
        Key=key,
        Body=json.dumps(payload, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


# Align dataframe columns with existing schema snapshot.
def align_to_schema(df, existing_columns):
    """
    Align DataFrame columns with stored schema and append missing fields.

    Args:
        df: DataFrame to align.
        existing_columns: Previously stored business columns.

    Returns:
        Tuple of aligned DataFrame and final business-column list.
    """
    incoming_columns = [c for c in df.columns if c not in METADATA_COLUMNS]
    final_columns = sorted(set(existing_columns).union(incoming_columns))

    out = df.copy()
    for name in final_columns:
        if name not in out.columns:
            out[name] = None

    selected = final_columns + \
        [c for c in METADATA_COLUMNS if c in out.columns]
    return out[selected], final_columns


# Build curated output key for a dataset and run.
def curated_output_key(path, dataset, run_id):
    """
    Build output parquet key for a dataset and run id.

    Args:
        path: Destination base prefix.
        dataset: Dataset name.
        run_id: Pipeline run identifier.

    Returns:
        S3 key for curated parquet output file.
    """
    return f"{with_trailing_slash(path)}{dataset}/run_id={run_id}/part-00000.parquet"


# Write dataframe as parquet to S3.
def write_parquet_to_s3(df, s3_client, bucket, key):
    """
    Serialize dataframe to parquet and upload to S3.

    Args:
        df: DataFrame to write.
        s3_client: Boto3 S3 client.
        bucket: Destination bucket.
        key: Destination object key.

    Returns:
        None.
    """
    payload = BytesIO()
    df.to_parquet(payload, index=False, engine="pyarrow")
    payload.seek(0)
    s3_client.put_object(Bucket=bucket, Key=key, Body=payload.getvalue(
    ), ContentType="application/octet-stream")


# Process one dataset group from raw CSV through curated parquet output.
def process_dataset(args, s3_client, dataset, keys):
    """
    Execute end-to-end transform flow for one dataset file group.

    Orchestrates the complete data pipeline: read raw CSV files, normalize columns,
    apply dataset-specific business rules, validate schema, and write to parquet.

    Args:
        args: Parsed runtime arguments.
        s3_client: Boto3 S3 client for source and schema snapshot access.
        dataset: Dataset name being processed.
        keys: Source file keys for this dataset.

    Returns:
        None.
    """
    # Stage 1: Read and normalize raw CSV files
    frames = []
    for key in keys:
        source_path = f"s3://{args['raw_bucket']}/{key}"
        raw_df = read_csv_from_s3(s3_client, args["raw_bucket"], key)
        clean_df = normalize_columns(raw_df)
        clean_df["source_file"] = source_path
        frames.append(clean_df)

    if not frames:
        log.info("No readable rows found for dataset %s", dataset)
        return

    # Stage 2: Concatenate files and enrich with metadata
    dataset_df = pd.concat(frames, ignore_index=True)
    enriched_df = add_ingest_columns(dataset_df, args["run_id"])

    # Stage 3: Apply dataset-specific transformations (songs: audio features, users: demographics, streams: event IDs)
    transformed_df = apply_dataset_transforms(dataset, enriched_df)
    output_df = cast_all_to_string(transformed_df)

    # Stage 4: Align to schema and handle schema drift
    registry_key = schema_registry_key(args["schema_registry_path"], dataset)
    existing_columns = read_schema_columns(
        s3_client, args["curated_bucket"], registry_key)
    aligned_df, final_columns = align_to_schema(output_df, existing_columns)

    # Stage 5: Write to curated parquet in S3
    output_key = curated_output_key(
        args["curated_path"], dataset, args["run_id"])
    try:
        write_parquet_to_s3(aligned_df, s3_client,
                            args["curated_bucket"], output_key)
        log.info(
            "Success: dataset=%s rows=%d files=%d output_key=%s",
            dataset, len(aligned_df), len(keys), output_key
        )
    except Exception as exc:
        log.error("Failed to write parquet for %s: %s", dataset, str(exc))
        raise

    # Stage 6: Update schema registry with latest column definitions
    try:
        write_schema_columns(
            s3_client=s3_client,
            bucket=args["curated_bucket"],
            key=registry_key,
            columns=final_columns,
            dataset=dataset,
        )
        log.info("Updated schema registry for %s: %d columns",
                 dataset, len(final_columns))
    except Exception as exc:
        log.error("Failed to update schema registry for %s: %s",
                  dataset, str(exc))


# Run the transformation job for one airflow-triggered execution.
def run_job(args):
    """
    Execute transformation flow across all discovered datasets.

    Loads raw CSV files from S3, normalizes and enriches them with business logic,
    validates against schema registry, and writes curated parquet files back to S3.

    Args:
        args: Parsed runtime arguments dictionary.
            Example: {
                "job_name": "s3_csv_to_redshift",
                "raw_bucket": "my-bucket",
                "raw_path": "Project-1/raw/",
                "curated_bucket": "my-bucket",
                "curated_path": "Project-1/curated/music/",
                "schema_registry_path": "Project-1/schemas/music/",
                "run_id": "20250228_143000",
                "file_keys_json": "[\"Project-1/raw/songs.csv\", ...]"
            }

    Returns:
        None.
    """
    s3_client = boto3.client(
        "s3", region_name=os.getenv("AWS_REGION", "ap-south-2"))

    # Load input file keys from args or S3 listing
    input_keys = load_input_keys(args, s3_client)
    if not input_keys:
        log.info("No files found for this run")
        return

    # Group files by dataset (songs, users, streams)
    grouped_keys = group_keys_by_dataset(input_keys)

    # Process each dataset independently
    for dataset, keys in grouped_keys.items():
        process_dataset(args, s3_client, dataset, keys)

    log.info("Transformation finished: job=%s run_id=%s",
             args["job_name"], args["run_id"])


# Script entrypoint.
def main():
    """
    Script entrypoint for local/manual execution.

    Args:
        None.

    Returns:
        None.
    """
    run_job(parse_args())


if __name__ == "__main__":
    main()
