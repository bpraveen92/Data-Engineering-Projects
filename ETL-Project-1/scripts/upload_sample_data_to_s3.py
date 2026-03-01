from __future__ import annotations

import os
from pathlib import Path

import boto3


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


# Normalize S3 path values to include a trailing slash.
def with_slash(prefix):
    """
    Ensure an S3 prefix string ends with '/'.

    Args:
        prefix: Prefix string to normalize.

    Returns:
        Normalized prefix ending with '/'.
    """
    return prefix if prefix.endswith("/") else f"{prefix}/"


# Upload all sample CSV files into the configured raw S3 path.
def main():
    """
    Upload local sample CSV files to configured raw S3 location.

    Args:
        None.

    Returns:
        None.
    """
    region = os.getenv("AWS_REGION", "ap-south-2")
    bucket = required("S3_RAW_BUCKET")
    raw_path = with_slash(required("S3_RAW_PREFIX"))

    sample_dir = Path("sample_data_synthetic")
    files = sorted(sample_dir.glob("*.csv"))
    if not files:
        raise ValueError("No CSV files found in sample_data_synthetic/")

    s3 = boto3.session.Session(region_name=region).client("s3")

    for path in files:
        key = f"{raw_path}{path.name}"
        s3.upload_file(str(path), bucket, key)
        print(f"ok: uploaded {path} -> s3://{bucket}/{key}")


if __name__ == "__main__":
    main()
