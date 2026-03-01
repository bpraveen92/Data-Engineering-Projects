from __future__ import annotations

import json
from datetime import datetime, timedelta

import boto3
import pendulum
from airflow.decorators import dag, task
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import get_current_context
from botocore.exceptions import ClientError

from src.common.config import env, load_pipeline_config
from src.common.datasets import dataset_from_key, group_keys_by_dataset

CONFIG = load_pipeline_config()
MAX_MANIFEST_KEYS = 100000


# Normalize S3-style path values.
def with_trailing_slash(path):
    """
    Ensure path-style strings always end with '/'.

    Args:
        path: Path string to normalize.
            Example: "Project-1/raw" or "Project-1/schemas/"

    Returns:
        Normalized path ending with '/'.
            Example: "Project-1/raw/" from input "Project-1/raw"
            Example: "Project-1/schemas/" from input "Project-1/schemas/"
    """
    return path if path.endswith("/") else f"{path}/"


# Read config keys with backward compatibility for older names.
def s3_env_name(primary, fallback):
    """
    Resolve config key names while supporting legacy key aliases.

    Args:
        primary: Preferred config key name.
            Example: "schema_registry_path_env"
        fallback: Legacy config key name.
            Example: "schema_registry_prefix_env"

    Returns:
        Environment-variable key configured for the S3 setting.
            Example: "S3_SCHEMA_REGISTRY_PREFIX"
    """
    s3_cfg = CONFIG["s3"]
    return s3_cfg.get(primary) or s3_cfg[fallback]


# Build the S3 key where Airflow keeps track of processed raw files.
def processed_manifest_key():
    """
    Build S3 key for Airflow processed-file state manifest.

    Args:
        None.

    Returns:
        S3 key path for processed raw files manifest JSON.
            Example: "Project-1/schemas/music/airflow/processed_raw_files.json"
    """
    schema_path = env(s3_env_name("schema_registry_path_env",
                      "schema_registry_prefix_env"))
    return f"{with_trailing_slash(schema_path)}airflow/processed_raw_files.json"


# Return all CSV files currently present under the raw path.
def list_raw_csv_files():
    """
    List all CSV object keys currently present under raw S3 path.

    Args:
        None.

    Returns:
        Ordered list of raw CSV keys sorted by LastModified timestamp.
            Example: [
                "Project-1/raw/songs.csv",
                "Project-1/raw/users.csv",
                "Project-1/raw/streams_20250228.csv"
            ]
    """
    s3 = boto3.client("s3", region_name=env("AWS_REGION", "ap-south-2"))
    raw_bucket = env(CONFIG["s3"]["raw_bucket_env"])
    raw_path = env(s3_env_name("raw_path_env", "raw_prefix_env"))

    objects = []
    token = None
    while True:
        request = {"Bucket": raw_bucket, "Prefix": raw_path}
        if token:
            request["ContinuationToken"] = token

        response = s3.list_objects_v2(**request)
        for item in response.get("Contents", []):
            key = item["Key"]
            if not key.endswith(".csv"):
                continue
            objects.append({"key": key, "last_modified": item["LastModified"]})

        if not response.get("IsTruncated"):
            break
        token = response.get("NextContinuationToken")

    objects.sort(key=lambda entry: entry["last_modified"])
    return [entry["key"] for entry in objects]


# Read processed raw-file keys from the Airflow state manifest.
def read_processed_files():
    """
    Read processed raw file keys from the Airflow S3 manifest.

    Args:
        None.

    Returns:
        List of raw file keys already processed by successful DAG runs.
            Example: [
                "Project-1/raw/songs.csv",
                "Project-1/raw/users.csv"
            ]
            Returns empty list if manifest doesn't exist yet.
    """
    s3 = boto3.client("s3", region_name=env("AWS_REGION", "ap-south-2"))
    curated_bucket = env(CONFIG["s3"]["curated_bucket_env"])
    key = processed_manifest_key()

    try:
        response = s3.get_object(Bucket=curated_bucket, Key=key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") == "NoSuchKey":
            return []
        raise

    payload = json.loads(response["Body"].read().decode("utf-8"))
    return payload.get("keys", [])


# Persist processed raw-file keys to S3 so each run only handles new arrivals.
def write_processed_files(processed_keys):
    """
    Write processed raw file keys to the Airflow S3 manifest.

    Args:
        processed_keys: List of raw file keys treated as processed.
            Example: ["Project-1/raw/songs.csv", "Project-1/raw/users.csv"]

    Returns:
        None.
    """
    s3 = boto3.client("s3", region_name=env("AWS_REGION", "ap-south-2"))
    curated_bucket = env(CONFIG["s3"]["curated_bucket_env"])
    key = processed_manifest_key()

    payload = {
        "updated_at_utc": datetime.utcnow().isoformat() + "Z",
        "count": len(processed_keys),
        "keys": processed_keys,
    }
    s3.put_object(
        Bucket=curated_bucket,
        Key=key,
        Body=json.dumps(payload, indent=2).encode("utf-8"),
        ContentType="application/json",
    )


# Merge existing and newly loaded keys while keeping manifest size bounded.
def merge_processed_keys(existing_keys, new_keys):
    """Deduplicate and merge processed file keys, capping at MAX_MANIFEST_KEYS."""
    merged = list(dict.fromkeys(existing_keys + new_keys))
    return merged[-MAX_MANIFEST_KEYS:] if len(merged) > MAX_MANIFEST_KEYS else merged


# Return raw CSV files that have not been processed yet.
def list_pending_csv_files():
    """
    Compute raw CSV files that still need processing.

    Args:
        None.

    Returns:
        List of pending raw CSV keys not present in processed manifest.
            Example: ["Project-1/raw/users.csv", "Project-1/raw/streams.csv"]
            Returns empty list if all files already processed.
    """
    all_keys = list_raw_csv_files()
    processed = set(read_processed_files())
    return [key for key in all_keys if key not in processed]


# Build curated S3 dataset location for a given run.
def curated_dataset_path(run_context, dataset):
    """
    Build curated dataset parquet path for a specific run.

    Args:
        run_context: Dictionary containing run_id.
            Example: {"run_id": "20250228_143000"}
        dataset: Dataset name to resolve output path.
            Example: "songs"

    Returns:
        Full S3 URI to dataset parquet output for the run.
            Example: "s3://pravbala-curated/Project-1/curated/songs/run_id=20250228_143000/"
    """
    curated_bucket = env(CONFIG["s3"]["curated_bucket_env"])
    curated_path = with_trailing_slash(
        env(s3_env_name("curated_path_env", "curated_prefix_env")))
    return f"s3://{curated_bucket}/{curated_path}{dataset}/run_id={run_context['run_id']}/"


# Build curated parquet file location for a given run.
def curated_dataset_file_path(run_context, dataset):
    """
    Build curated dataset parquet file path for a specific run.

    Args:
        run_context: Dictionary containing run_id.
            Example: {"run_id": "20250228_143000"}
        dataset: Dataset name to resolve output file path.
            Example: "songs"

    Returns:
        Full S3 URI to dataset parquet file output for the run.
            Example: "s3://pravbala-curated/Project-1/curated/songs/run_id=20250228_143000/part-00000.parquet"
    """
    return f"{curated_dataset_path(run_context, dataset)}part-00000.parquet"


@dag(
    dag_id="s3_to_redshift_pipeline",
    schedule=env("BATCH_SCHEDULE", CONFIG["pipeline"]["schedule"]),
    start_date=pendulum.datetime(2026, 2, 20, tz="UTC"),
    catchup=CONFIG["pipeline"]["catchup"],
    max_active_runs=CONFIG["pipeline"]["max_active_runs"],
    dagrun_timeout=timedelta(hours=1),
    default_args={"owner": "data-eng", "retries": 1,
                  "retry_delay": timedelta(minutes=5)},
    tags=["s3", "python-transform", "redshift", "great-expectations", "mwaa"],
)
def s3_to_redshift_pipeline():
    """
    Define Airflow DAG for scheduled S3-to-Redshift pipeline orchestration.

    Orchestrates the full data pipeline: discovers pending raw CSV files, transforms to curated parquet,
    validates quality, loads to Redshift silver layer, refreshes gold layer, and updates processed manifest.

    Args:
        None.

    Returns:
        Airflow task graph generated by decorator execution.
            Example: DAG with 11 tasks: start -> find_pending_files -> should_run -> build_run_context -> 
            run_transform -> run_quality_checks -> load_to_redshift -> build_gold_layer -> 
            mark_files_processed -> end
    """
    # Discover raw files that have not yet been processed.
    @task
    def find_pending_files():
        """
        Resolve pending source files and keep only supported dataset files.

        Args:
            None.

        Returns:
            List of raw CSV keys ready for this run.
                Example: ["Project-1/raw/songs.csv", "Project-1/raw/users.csv"]
        """
        pending_keys = list_pending_csv_files()
        return [key for key in pending_keys if dataset_from_key(key)]

    # Skip the run quickly if no files arrived.
    @task.short_circuit
    def should_run(file_keys):
        """
        Stop downstream work when no pending files are available.

        Args:
            file_keys: List of pending raw file keys.
                Example: ["Project-1/raw/songs.csv"]

        Returns:
            True when pipeline should continue, otherwise False.
                Example: True (when files present) or False (when empty list)
        """
        return bool(file_keys)

    # Build context shared across downstream tasks.
    @task
    def build_run_context(file_keys):
        """
        Create run-scoped context payload used by downstream tasks.

        Args:
            file_keys: List of pending raw file keys for the run.
                Example: ["Project-1/raw/songs.csv", "Project-1/raw/users.csv"]

        Returns:
            Dictionary containing run id and serialized dataset grouping.
                Example: {
                    "run_id": "20250228_143000",
                    "file_keys_json": "[\"Project-1/raw/songs.csv\", \"Project-1/raw/users.csv\"]",
                    "grouped_keys_json": "{\"songs\": [\"Project-1/raw/songs.csv\"], \"users\": [\"Project-1/raw/users.csv\"]}"
                }
        """
        ctx = get_current_context()
        grouped_keys = group_keys_by_dataset(file_keys)
        return {
            "run_id": ctx["ts_nodash"],
            "file_keys_json": json.dumps(file_keys),
            "grouped_keys_json": json.dumps(grouped_keys),
        }

    # Run Python transformation job for the pending file set.
    @task
    def run_transform(run_context):
        """
        Run transformation script logic and return current run id.

        Invokes csv_to_curated_transform_job to convert pending raw CSV files to normalized parquet
        with schema inference and column name standardization.

        Args:
            run_context: Dictionary with serialized run metadata.
                Example: {
                    "run_id": "20250228_143000",
                    "file_keys_json": "[\"Project-1/raw/songs.csv\", \"Project-1/raw/users.csv\"]",
                    "grouped_keys_json": "{\"songs\": [...], \"users\": [...]}"
                }

        Returns:
            Run id string.
                Example: "20250228_143000"
        """
        transform_args = {
            "job_name": env("PIPELINE_NAME", "s3_csv_to_redshift"),
            "raw_bucket": env(CONFIG["s3"]["raw_bucket_env"]),
            "raw_path": env(s3_env_name("raw_path_env", "raw_prefix_env")),
            "curated_bucket": env(CONFIG["s3"]["curated_bucket_env"]),
            "curated_path": env(s3_env_name("curated_path_env", "curated_prefix_env")),
            "schema_registry_path": env(s3_env_name("schema_registry_path_env", "schema_registry_prefix_env")),
            "run_id": run_context["run_id"],
            "file_keys_json": run_context["file_keys_json"],
        }
        from transform_jobs.csv_to_curated_transform_job import run_job

        run_job(transform_args)
        return run_context["run_id"]

    # Validate curated parquet output with dataset-specific checks.
    @task
    def run_quality_checks(run_context):
        """
        Execute dataset-level quality checks on curated parquet outputs.

        Runs Great Expectations validation against each dataset's output parquet using quality_rules.yaml
        configuration. Respects failure_policy setting to either fail-fast or quarantine and continue.

        Args:
            run_context: Dictionary with run metadata and dataset grouping.
                Example: {
                    "run_id": "20250228_143000",
                    "file_keys_json": "[...]",
                    "grouped_keys_json": "{\"songs\": [...], \"users\": [...]}"
                }

        Returns:
            Dictionary of dataset names to quality-check result metadata.
                Example: {
                    "songs": {"success": True, "rows": 15234},
                    "users": {"success": True, "rows": 4567}
                }
        """
        grouped_keys = json.loads(run_context["grouped_keys_json"])
        datasets = list(grouped_keys.keys())
        failure_policy = env(
            CONFIG["quality"]["failure_policy_env"], "quarantine-and-continue")
        from src.quality.ge_validator import validate_curated_parquet

        results = {}
        for dataset in datasets:
            dataset_path = curated_dataset_path(run_context, dataset)
            try:
                summary = validate_curated_parquet(
                    parquet_uri=dataset_path, dataset=dataset)
                results[dataset] = {
                    "success": summary["success"], "rows": summary["checked_rows"]}
            except ValueError as exc:
                if failure_policy == "fail-fast":
                    raise
                results[dataset] = {"success": False,
                                    "rows": 0, "warning": str(exc)}

        return results

    # Load validated datasets into Redshift and track skipped ones.
    @task
    def load_to_redshift(run_context, quality_results):
        """
        Load quality-approved datasets to Redshift and track skipped sets.

        Loads quality-approved curated parquet datasets to the silver layer in Redshift using staging
        tables and deduplication. Skips datasets that failed quality checks based on failure_policy.

        Args:
            run_context: Dictionary with run metadata and dataset grouping.
                Example: {"run_id": "20250228_143000", "grouped_keys_json": "{...}"}
            quality_results: Dataset quality outcomes from prior task.
                Example: {"songs": {"success": True, "rows": 15234}, "users": {"success": True, "rows": 4567}}

        Returns:
            Dictionary containing loaded and skipped dataset lists.
                Example: {"loaded": ["songs", "users"], "skipped": []}
        """
        failure_policy = env(
            CONFIG["quality"]["failure_policy_env"], "quarantine-and-continue")
        grouped_keys = json.loads(run_context["grouped_keys_json"])
        datasets = list(grouped_keys.keys())
        from src.loaders.redshift_loader import load_curated_partition_to_redshift

        loaded = []
        skipped = []

        for dataset in datasets:
            quality_ok = quality_results.get(dataset, {}).get("success", False)

            if not quality_ok and failure_policy == "fail-fast":
                raise ValueError(f"Quality check failed for {dataset}.")

            if not quality_ok:
                skipped.append(dataset)
                continue

            load_curated_partition_to_redshift(
                run_id=run_context["run_id"], dataset=dataset)
            loaded.append(dataset)

        return {"loaded": loaded, "skipped": skipped}

    # Refresh BI-ready gold tables after successful silver loads.
    @task
    def build_gold_layer(run_context, load_summary):
        """
        Build and refresh gold reporting tables in Redshift.

        Creates or refreshes gold-layer aggregation tables based on successfully loaded silver datasets.
        Skips refresh if no datasets were loaded. Gold tables provide BI-ready aggregated views.

        Args:
            run_context: Dictionary with run metadata.
                Example: {"run_id": "20250228_143000"}
            load_summary: Dictionary with loaded/skipped dataset names.
                Example: {"loaded": ["songs", "users"], "skipped": []}

        Returns:
            Dictionary with gold refresh execution summary.
                Example: {"status": "success", "loaded": ["songs", "users"], "gold": ["songs_summary", "users_summary"]}
                Or: {"status": "skipped", "reason": "no datasets loaded"}
        """
        loaded = load_summary.get("loaded", [])
        if not loaded:
            return {"status": "skipped", "reason": "no datasets loaded"}

        from src.loaders.redshift_loader import refresh_gold_reporting_tables

        refreshed = refresh_gold_reporting_tables(run_id=run_context["run_id"])
        return {"status": "success", "loaded": loaded, "gold": refreshed}

    # Mark successfully loaded raw files as processed in the Airflow state manifest.
    @task
    def mark_files_processed(run_context, load_summary, gold_summary):
        """
        Persist successfully processed raw files to processed manifest in S3.

        Updates the processed manifest with all files that were processed in this run
        (whether they loaded successfully, failed quality checks, or were skipped).
        Deduplicates entries and caps manifest size at MAX_MANIFEST_KEYS to manage state file growth.

        Args:
            run_context: Dictionary with run metadata and grouped file keys.
                Example: {"run_id": "20250228_143000", "grouped_keys_json": "{\"songs\": [\"Project-1/raw/songs.csv\"]}"}
            load_summary: Dictionary with loaded/skipped dataset names.
                Example: {"loaded": ["songs"], "skipped": ["users"]}
            gold_summary: Dictionary with gold layer refresh status.
                Example: {"status": "success", "loaded": ["songs"]}

        Returns:
            Human-readable status string with manifest update stats.
                Example: "marked=3|manifest_count=8|gold=success"
        """
        grouped_keys = json.loads(run_context["grouped_keys_json"])

        # Mark ALL files that were processed in this run, including all datasets
        # This ensures songs, users, AND streams all get tracked, not just the loaded ones
        completed_keys = []
        for dataset, keys in grouped_keys.items():
            completed_keys.extend(keys)

        if not completed_keys:
            return "no files to mark as processed"

        existing_keys = read_processed_files()
        merged_keys = merge_processed_keys(existing_keys, completed_keys)
        write_processed_files(merged_keys)

        return f"marked={len(completed_keys)}|manifest_count={len(merged_keys)}|gold={gold_summary.get('status', 'unknown')}"

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    pending_files = find_pending_files()
    run_guard = should_run(pending_files)
    run_context = build_run_context(pending_files)

    transform_done = run_transform(run_context)

    quality_done = run_quality_checks(run_context)
    load_done = load_to_redshift(run_context, quality_done)
    gold_done = build_gold_layer(run_context, load_done)
    mark_done = mark_files_processed(run_context, load_done, gold_done)

    start >> pending_files >> run_guard >> run_context >> transform_done >> quality_done >> load_done >> gold_done >> mark_done >> end


dag_instance = s3_to_redshift_pipeline()
