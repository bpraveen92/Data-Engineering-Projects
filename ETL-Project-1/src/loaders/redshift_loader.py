from __future__ import annotations

import hashlib
import json
import os
import time

import boto3
from botocore.exceptions import ClientError

from src.common.config import env

METADATA_COLUMNS = ["run_id", "ingest_ts", "ingest_date", "source_file"]
DEFAULT_PRIMARY_KEYS = {
    "songs": "track_id",
    "users": "user_id",
    "streams": "stream_event_id",
}


# Build a dictionary with Redshift runtime settings for Data API calls.
def redshift_session():
    """Build Redshift connection configuration."""
    return {
        "database": env("REDSHIFT_DATABASE"),
        "workgroup": env("REDSHIFT_WORKGROUP"),
        "secret_arn": env("REDSHIFT_SECRET_ARN"),
        "iam_role_arn": env("REDSHIFT_IAM_ROLE_ARN"),
    }


# Build a Redshift Data API client in the configured AWS region.
def create_redshift_client():
    """Create a Redshift Data API client."""
    return boto3.client("redshift-data", region_name=env("AWS_REGION", "ap-south-2"))


# Build an S3 client in the configured AWS region.
def create_s3_client():
    """Create an S3 client."""
    return boto3.client("s3", region_name=env("AWS_REGION", "ap-south-2"))


# Split a fully qualified table name into schema and table parts.
def split_table_name(full_name):
    """Split schema.table into (schema, table) tuple."""
    if "." not in full_name:
        raise ValueError(f"Table name must be schema.table, got {full_name}")
    return tuple(full_name.split(".", 1))


# Submit a SQL statement and return the Data API statement id.
def run_statement(session, sql):
    """Submit SQL to Redshift Data API and return statement id."""
    response = create_redshift_client().execute_statement(
        WorkgroupName=session["workgroup"],
        Database=session["database"],
        SecretArn=session["secret_arn"],
        Sql=sql,
    )
    return response["Id"]


# Poll a Data API statement until it succeeds or fails.
def wait_for_statement(statement_id, timeout_seconds=900):
    """Poll Redshift Data API until statement finishes or times out."""
    client = create_redshift_client()
    started = time.time()

    while True:
        status = client.describe_statement(Id=statement_id)["Status"]

        if status == "FINISHED":
            return

        if status in {"FAILED", "ABORTED"}:
            details = client.describe_statement(Id=statement_id)
            raise RuntimeError(f"Redshift statement failed: {details}")

        if time.time() - started > timeout_seconds:
            raise TimeoutError(
                f"Redshift statement timed out after {timeout_seconds}s")

        time.sleep(3)


# Convenience wrapper to run SQL and wait for completion.
def execute_sql(session, sql, timeout_seconds=900):
    """Execute SQL and block until completion."""
    statement_id = run_statement(session, sql)
    wait_for_statement(statement_id, timeout_seconds=timeout_seconds)


# Run a query and return values from the first column of each row.
def query_first_column(session, sql):
    """Run a query and return the first column value from each row."""
    statement_id = run_statement(session, sql)
    wait_for_statement(statement_id)
    result = create_redshift_client().get_statement_result(Id=statement_id)

    values = []
    for row in result.get("Records", []):
        if row:
            values.append(next(iter(row[0].values())))
    return values


# Make sure S3-like path values end with a trailing slash.
def ensure_trailing_slash(path):
    """Ensure path-style strings always end with '/'."""
    return path if path.endswith("/") else f"{path}/"


# Resolve the destination table for a given dataset.
def get_target_table(dataset):
    """Resolve destination Redshift table for a dataset (silver layer)."""
    return f"{env('TARGET_SCHEMA', 'silver')}.{dataset}"


# Resolve schema used for BI-ready gold reporting tables.
def get_gold_schema():
    """Resolve gold schema name for BI-ready tables."""
    return env("GOLD_SCHEMA", "gold")


# Build the schema snapshot key path for a dataset.
def get_schema_registry_key(dataset):
    """Build S3 key for schema snapshot of a dataset."""
    schema_path = ensure_trailing_slash(env("S3_SCHEMA_REGISTRY_PREFIX"))
    return f"{schema_path}{dataset}_schema.json"


# Resolve primary key, allowing environment overrides per dataset.
def get_primary_key(dataset):
    """Get primary key for dataset from env override or defaults."""
    override = os.getenv(f"PRIMARY_KEY_{dataset.upper()}")
    return override or DEFAULT_PRIMARY_KEYS.get(dataset)


# Load schema columns for a dataset from S3 schema snapshot.
def load_schema_columns_from_s3(curated_bucket, dataset):
    """Read stored schema columns for dataset from S3 snapshot."""
    key = get_schema_registry_key(dataset)
    try:
        response = create_s3_client().get_object(Bucket=curated_bucket, Key=key)
    except ClientError as exc:
        if exc.response.get("Error", {}).get("Code") != "NoSuchKey":
            raise
        return []

    payload = json.loads(response["Body"].read().decode("utf-8"))
    return payload.get("columns", [])


# Create base dataset table and audit table if they do not exist.
def ensure_tables_exist(session, table_name):
    """Create required Redshift schema, target table, and audit table."""
    schema, table = split_table_name(table_name)
    create_sql = f"""
        CREATE SCHEMA IF NOT EXISTS {schema};

        CREATE TABLE IF NOT EXISTS {schema}.{table} (
            run_id VARCHAR(128),
            ingest_ts VARCHAR(256),
            ingest_date VARCHAR(32),
            source_file VARCHAR(4096)
        );

        CREATE TABLE IF NOT EXISTS {schema}.pipeline_audit (
            run_id VARCHAR(128),
            dataset VARCHAR(64),
            source_count BIGINT,
            loaded_at TIMESTAMP DEFAULT GETDATE(),
            status VARCHAR(64),
            details VARCHAR(65535)
        );
        """
    execute_sql(session, create_sql)

    # Ensure metadata columns have adequate size
    try:
        execute_sql(
            session, f"ALTER TABLE {schema}.{table} ALTER COLUMN ingest_ts TYPE VARCHAR(256);")
    except RuntimeError as exc:
        if "target column size should be different" not in str(exc):
            raise

    try:
        execute_sql(
            session, f"ALTER TABLE {schema}.{table} ALTER COLUMN source_file TYPE VARCHAR(4096);")
    except RuntimeError as exc:
        if "target column size should be different" not in str(exc):
            raise


# Read current table columns from Redshift information schema.
def get_existing_columns(session, table_name):
    """Fetch current Redshift columns for a target table."""
    schema, table = split_table_name(table_name)
    sql = f"""
    SELECT column_name
    FROM information_schema.columns
    WHERE table_schema = '{schema}' AND table_name = '{table}'
    """
    return set(query_first_column(session, sql))


# Add any newly discovered business columns to the target table.
def ensure_data_columns_exist(session, table_name, wanted_columns):
    """Add missing business columns to the target Redshift table."""
    existing = get_existing_columns(session, table_name)
    statements = []

    for column in wanted_columns:
        if column in METADATA_COLUMNS or column in existing:
            continue
        statements.append(
            f"ALTER TABLE {table_name} ADD COLUMN {column} VARCHAR(65535);")

    if statements:
        execute_sql(session, "\n".join(statements))


# Build a deterministic staging table name from dataset and run id.
def build_staging_table_name(target_table, run_id, dataset):
    """Build deterministic staging table name for one dataset run."""
    schema, table = split_table_name(target_table)
    suffix = hashlib.sha1(
        f"{dataset}:{run_id}".encode("utf-8")).hexdigest()[:10]
    return f"{schema}.{table}_stg_{suffix}"


# Build explicit column list for COPY so column order is always stable.
def build_copy_column_list(schema_columns):
    """Build explicit Redshift COPY column list including metadata fields."""
    ordered = list(schema_columns) + METADATA_COLUMNS
    return ", ".join(ordered)


# Build dedupe SQL for datasets that have a configured primary key.
def build_delete_sql_by_primary_key(target_table, staging_table, dataset, session):
    """Build dedupe DELETE SQL if dataset has a valid primary key."""
    primary_key = get_primary_key(dataset)
    if not primary_key:
        return ""

    existing = get_existing_columns(session, target_table)
    if primary_key not in existing:
        return ""

    return f"""
    DELETE FROM {target_table}
    USING {staging_table}
    WHERE {target_table}.{primary_key} = {staging_table}.{primary_key};
    """


# Load one dataset/run partition from curated S3 into Redshift.
def load_curated_partition_to_redshift(run_id, dataset):
    """Load one curated dataset partition from S3 to Redshift with upsert."""
    session = redshift_session()

    curated_bucket = env("S3_CURATED_BUCKET")
    curated_path = ensure_trailing_slash(env("S3_CURATED_PREFIX"))
    target_table = get_target_table(dataset)
    staging_table = build_staging_table_name(target_table, run_id, dataset)

    ensure_tables_exist(session, target_table)

    schema_columns = load_schema_columns_from_s3(curated_bucket, dataset)
    ensure_data_columns_exist(session, target_table, schema_columns)

    source_path = f"s3://{curated_bucket}/{curated_path}{dataset}/run_id={run_id}/"
    execute_sql(session, f"DROP TABLE IF EXISTS {staging_table};")
    execute_sql(
        session, f"CREATE TABLE {staging_table} (LIKE {target_table});")

    try:
        copy_columns = build_copy_column_list(schema_columns)
        copy_sql = f"""
        COPY {staging_table} ({copy_columns})
        FROM '{source_path}'
        IAM_ROLE '{session["iam_role_arn"]}'
        FORMAT AS PARQUET;
        """
        execute_sql(session, copy_sql)

        delete_sql = build_delete_sql_by_primary_key(
            target_table, staging_table, dataset, session)
        schema_name, _ = split_table_name(target_table)

        upsert_sql = f"""
        {delete_sql}

        INSERT INTO {target_table}
        SELECT * FROM {staging_table};

        INSERT INTO {schema_name}.pipeline_audit (run_id, dataset, source_count, status, details)
        SELECT '{run_id}', '{dataset}', COUNT(*), 'success', 'Loaded from {source_path}'
        FROM {staging_table};
        """
        execute_sql(session, upsert_sql)
    finally:
        execute_sql(session, f"DROP TABLE IF EXISTS {staging_table};")


# Create gold summary tables used by BI dashboards.
def ensure_gold_tables_exist(session):
    """Create BI-facing gold tables if they do not exist."""
    gold_schema = get_gold_schema()
    create_sql = f"""
    CREATE SCHEMA IF NOT EXISTS {gold_schema};

    CREATE TABLE IF NOT EXISTS {gold_schema}.daily_stream_metrics (
      metric_date DATE,
      total_streams BIGINT,
      unique_users BIGINT,
      unique_tracks BIGINT,
      updated_at TIMESTAMP DEFAULT GETDATE()
    );

    CREATE TABLE IF NOT EXISTS {gold_schema}.top_tracks_daily (
      metric_date DATE,
      track_id VARCHAR(256),
      track_name VARCHAR(65535),
      artist_name VARCHAR(65535),
      stream_count BIGINT,
      rank_in_day INTEGER,
      updated_at TIMESTAMP DEFAULT GETDATE()
    );

    CREATE TABLE IF NOT EXISTS {gold_schema}.country_streams_daily (
      metric_date DATE,
      user_country VARCHAR(256),
      stream_count BIGINT,
      unique_users BIGINT,
      updated_at TIMESTAMP DEFAULT GETDATE()
    );
    """
    execute_sql(session, create_sql)


# Refresh gold reporting tables from silver layer data.
def refresh_gold_reporting_tables(run_id):
    """Rebuild BI-ready gold summary tables using latest silver datasets."""
    session = redshift_session()
    silver = env("TARGET_SCHEMA", "silver")
    gold = get_gold_schema()

    ensure_gold_tables_exist(session)

    sql = f"""
    INSERT INTO {gold}.daily_stream_metrics (metric_date, total_streams, unique_users, unique_tracks, updated_at)
    SELECT
      DATE_TRUNC('day', TRY_CAST(NULLIF(listen_time, '') AS TIMESTAMP))::date,
      COUNT(*)::bigint,
      COUNT(DISTINCT NULLIF(user_id, ''))::bigint,
      COUNT(DISTINCT NULLIF(track_id, ''))::bigint,
      GETDATE()
    FROM {silver}.streams
    WHERE NULLIF(listen_time, '') IS NOT NULL
    GROUP BY 1;

    INSERT INTO {gold}.top_tracks_daily (metric_date, track_id, track_name, artist_name, stream_count, rank_in_day, updated_at)
    WITH day_tracks AS (
      SELECT
        DATE_TRUNC('day', TRY_CAST(NULLIF(listen_time, '') AS TIMESTAMP))::date AS metric_date,
        NULLIF(track_id, '') AS track_id,
        COUNT(*)::bigint AS stream_count
      FROM {silver}.streams
      WHERE NULLIF(listen_time, '') IS NOT NULL AND NULLIF(track_id, '') IS NOT NULL
      GROUP BY 1, 2
    ),
    ranked_songs AS (
      SELECT
        NULLIF(track_id, '') AS track_id,
        NULLIF(track_name, '') AS track_name,
        NULLIF(artists, '') AS artists,
        ROW_NUMBER() OVER (PARTITION BY NULLIF(track_id, '') ORDER BY TRY_CAST(NULLIF(ingest_ts, '') AS TIMESTAMP) DESC NULLS LAST) AS rn
      FROM {silver}.songs
    ),
    ranked_tracks AS (
      SELECT
        d.metric_date,
        d.track_id,
        COALESCE(s.track_name, 'unknown') AS track_name,
        COALESCE(s.artists, 'unknown') AS artist_name,
        d.stream_count,
        ROW_NUMBER() OVER (PARTITION BY d.metric_date ORDER BY d.stream_count DESC)::int AS rank_in_day
      FROM day_tracks d
      LEFT JOIN ranked_songs s ON s.track_id = d.track_id AND s.rn = 1
    )
    SELECT
      metric_date,
      track_id,
      track_name,
      artist_name,
      stream_count,
      rank_in_day,
      GETDATE()
    FROM ranked_tracks
    WHERE rank_in_day <= 10;

    INSERT INTO {gold}.country_streams_daily (metric_date, user_country, stream_count, unique_users, updated_at)
    WITH ranked_users AS (
      SELECT
        NULLIF(user_id, '') AS user_id,
        NULLIF(user_country, '') AS user_country,
        ROW_NUMBER() OVER (PARTITION BY NULLIF(user_id, '') ORDER BY TRY_CAST(NULLIF(ingest_ts, '') AS TIMESTAMP) DESC NULLS LAST) AS rn
      FROM {silver}.users
    )
    SELECT
      DATE_TRUNC('day', TRY_CAST(NULLIF(listen_time, '') AS TIMESTAMP))::date,
      COALESCE(u.user_country, 'unknown'),
      COUNT(*)::bigint,
      COUNT(DISTINCT s.user_id)::bigint,
      GETDATE()
    FROM {silver}.streams s
    LEFT JOIN ranked_users u ON u.user_id = s.user_id AND u.rn = 1
    WHERE TRY_CAST(NULLIF(s.listen_time, '') AS TIMESTAMP) IS NOT NULL
    GROUP BY 1, 2;
    """
    execute_sql(session, sql, timeout_seconds=1800)

    return ["daily_stream_metrics", "top_tracks_daily", "country_streams_daily"]
