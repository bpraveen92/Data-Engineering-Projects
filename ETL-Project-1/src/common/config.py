from __future__ import annotations

import json
import os
import time
from pathlib import Path

import yaml


PROJECT_ROOT = Path(__file__).resolve().parents[2]
PIPELINE_CONFIG_PATH = PROJECT_ROOT / "config" / "pipeline_config.yaml"
QUALITY_RULES_PATH = PROJECT_ROOT / "config" / "quality_rules.yaml"

# Secrets Manager cache with TTL to minimize API calls.
SECRETS_CACHE = {}
CACHE_TTL_SECONDS = 300  # 5 minutes
CACHE_TIMESTAMP = 0
SECRETS_MANAGER_AVAILABLE = None


# Load YAML config from disk into a dict.
def load_yaml(path):
    """
    Read a YAML file from disk and parse it as a dictionary.

    Args:
        path: Path object pointing to a YAML config file.
            Example: Path("config/pipeline_config.yaml")

    Returns:
        Parsed YAML content as a dictionary-like object.
            Example: {
                "pipeline_name": "s3_csv_to_redshift",
                "schedule": "*/15 * * * *",
                "failure_policy": "quarantine-and-continue"
            }
    """
    with path.open("r", encoding="utf-8") as handle:
        return yaml.safe_load(handle)


# Load top-level pipeline configuration.
def load_pipeline_config():
    """
    Load the main pipeline configuration file.

    Args:
        None.

    Returns:
        Parsed pipeline configuration values.
            Example: {
                "pipeline_name": "s3_csv_to_redshift",
                "schedule_interval": "*/15 * * * *",
                "failure_policy": "quarantine-and-continue",
                "datasets": ["songs", "users", "streams"]
            }
    """
    return load_yaml(PIPELINE_CONFIG_PATH)


# Load data-quality rules configuration.
def load_quality_rules():
    """
    Load data quality rules used by Great Expectations checks.

    Args:
        None.

    Returns:
        Parsed quality rules configuration.
            Example: {
                "songs": {
                    "required_columns": ["track_id", "track_name", "artists"],
                    "null_threshold": 0.05,
                    "unique_columns": ["track_id"]
                },
                "users": {
                    "required_columns": ["user_id", "user_name"],
                    "null_threshold": 0.10
                }
            }
    """
    return load_yaml(QUALITY_RULES_PATH)


# Read an environment variable with optional default and strict missing check.
def load_secrets_from_secrets_manager(secret_name="project1-config"):
    """
    Load configuration from AWS Secrets Manager and cache the result.

    This function is used as a fallback when environment variables are not found
    locally (e.g., on MWAA where .env is not available). The secrets are cached
    for 5 minutes to minimize API calls and avoid rate limiting.

    Args:
        secret_name: Name of the secret in AWS Secrets Manager.
            Example: "project1-config"

    Returns:
        Dictionary containing all configuration key-value pairs from the secret.
            Example: {
                "AWS_REGION": "ap-south-2",
                "S3_RAW_BUCKET": "pravbala-data-engineering-projects",
                "TARGET_SCHEMA": "silver",
                "REDSHIFT_DATABASE": "dev"
            }

    Raises:
        RuntimeError: If Secrets Manager is unavailable or the secret cannot be retrieved.
    """
    global SECRETS_CACHE, CACHE_TIMESTAMP

    current_time = time.time()
    cache_age = current_time - CACHE_TIMESTAMP

    # Return cached secrets if fresh.
    if SECRETS_CACHE and cache_age < CACHE_TTL_SECONDS:
        return SECRETS_CACHE

    try:
        import boto3
        from botocore.exceptions import ClientError
    except ImportError as exc:
        raise RuntimeError(
            "boto3 is required for Secrets Manager integration but not installed") from exc

    try:
        client = boto3.client("secretsmanager")
        response = client.get_secret_value(SecretId=secret_name)
        secret_string = response.get("SecretString")

        if not secret_string:
            raise RuntimeError(
                f"Secrets Manager secret '{secret_name}' is empty")

        secrets = json.loads(secret_string)
        SECRETS_CACHE = secrets
        CACHE_TIMESTAMP = current_time

        return secrets

    except ClientError as exc:
        error_code = exc.response.get("Error", {}).get("Code", "Unknown")
        if error_code == "ResourceNotFoundException":
            raise RuntimeError(
                f"Secrets Manager secret '{secret_name}' not found. "
                f"Please create it with all required configuration variables."
            ) from exc
        raise RuntimeError(
            f"Failed to retrieve secret from Secrets Manager: {error_code}") from exc
    except Exception as exc:
        raise RuntimeError(
            f"Unexpected error loading secrets from Secrets Manager: {exc}") from exc


def env(name, default=None):
    """
    Read environment variable with fallback to AWS Secrets Manager.

    Implements three-stage lookup:
    1. Local environment variables (.env file or system env) - fast path
    2. AWS Secrets Manager (for MWAA) - cached for 5 minutes
    3. Provided default value or raise error
    """
    # Stage 1: Check local environment
    value = os.getenv(name)
    if value is not None:
        return value

    # Stage 2: Try Secrets Manager (for MWAA deployments)
    global SECRETS_MANAGER_AVAILABLE
    if SECRETS_MANAGER_AVAILABLE is None:
        try:
            import boto3
            SECRETS_MANAGER_AVAILABLE = True
        except ImportError:
            SECRETS_MANAGER_AVAILABLE = False

    if SECRETS_MANAGER_AVAILABLE:
        try:
            secrets = load_secrets_from_secrets_manager()
            value = secrets.get(name)
            if value is not None:
                return value
        except RuntimeError:
            pass

    # Stage 3: Use default or raise error
    if default is not None:
        return default

    raise ValueError(f"Missing required environment variable: {name}")
