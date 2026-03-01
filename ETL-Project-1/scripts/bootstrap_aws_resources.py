from __future__ import annotations

import os

import boto3


# Build bootstrap settings dictionary from environment values.
def build_settings(
    region,
    raw_bucket,
    raw_path,
    curated_bucket,
    curated_path,
    schema_path,
):
    """
    Store AWS bootstrap settings in a plain dictionary.

    Args:
        region: AWS region for all service calls.
        raw_bucket: S3 bucket holding raw input files.
        raw_path: S3 prefix used for raw data.
        curated_bucket: S3 bucket holding curated output and schemas.
        curated_path: S3 prefix for curated data.
        schema_path: S3 prefix for schema snapshots.

    Returns:
        Dictionary containing bootstrap settings.
    """
    return {
        "region": region,
        "raw_bucket": raw_bucket,
        "raw_path": raw_path,
        "curated_bucket": curated_bucket,
        "curated_path": curated_path,
        "schema_path": schema_path,
    }


# Read a required environment variable.
def required(name):
    """
    Read a required environment variable and fail when missing.

    Args:
        name: Environment variable name.

    Returns:
        Environment variable value.
    """
    value = os.getenv(name)
    if not value:
        raise ValueError(f"Missing required env var: {name}")
    return value


# Normalize S3-like paths to always end with '/'.
def with_trailing_slash(path):
    """
    Ensure path-style strings always end with '/'.

    Args:
        path: Path string to normalize.

    Returns:
        Normalized path ending with '/'.
    """
    return path if path.endswith("/") else f"{path}/"


# Build typed settings from environment variables.
def load_settings():
    """
    Build settings object from required environment variables.

    Args:
        None.

    Returns:
        Dictionary containing bootstrap configuration.
    """
    return build_settings(
        region=os.getenv("AWS_REGION", "ap-south-2"),
        raw_bucket=required("S3_RAW_BUCKET"),
        raw_path=with_trailing_slash(required("S3_RAW_PREFIX")),
        curated_bucket=required("S3_CURATED_BUCKET"),
        curated_path=with_trailing_slash(required("S3_CURATED_PREFIX")),
        schema_path=with_trailing_slash(required("S3_SCHEMA_REGISTRY_PREFIX")),
    )


# Create a tiny placeholder object to materialize the path in S3.
def ensure_path_marker(s3, bucket, path):
    """
    Create a lightweight marker object so an S3 path is visible.

    Args:
        s3: Boto3 S3 client.
        bucket: Target bucket name.
        path: S3 prefix to materialize.

    Returns:
        None.
    """
    key = f"{path}.keep"
    s3.put_object(Bucket=bucket, Key=key, Body=b"", ContentType="text/plain")
    print(f"ok: s3://{bucket}/{key}")


# Run end-to-end AWS bootstrap setup.
def main():
    """
    Execute bootstrap flow for required S3 paths.

    Args:
        None.

    Returns:
        None.
    """
    settings = load_settings()
    session = boto3.session.Session(region_name=settings["region"])
    s3 = session.client("s3")

    ensure_path_marker(s3, settings["raw_bucket"], settings["raw_path"])
    ensure_path_marker(
        s3, settings["curated_bucket"], settings["curated_path"])
    ensure_path_marker(s3, settings["curated_bucket"], settings["schema_path"])


if __name__ == "__main__":
    main()
