from __future__ import annotations

import json
import logging

import pandas as pd
from great_expectations.dataset import PandasDataset

from src.common.config import load_quality_rules

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)
# Fetch quality rules for the target dataset.


def rules_for_dataset(dataset):
    """
    Fetch quality rules for a specific dataset from config.

    Args:
        dataset: Dataset name like songs, users, or streams.
            Example: "songs" or "users" or "streams"

    Returns:
        Dictionary containing validation rules for that dataset.
            Example: {
                "required_columns": ["track_id", "track_name", "artists"],
                "max_null_ratio": {"track_name": 0.05},
                "unique_columns": ["track_id"]
            }
    """
    all_rules = load_quality_rules().get("datasets", {})
    rules = all_rules.get(dataset)
    if not rules:
        raise ValueError(f"Missing quality rules for dataset '{dataset}'")
    return rules


# Wrap a Pandas DataFrame in Great Expectations dataset API.
def as_ge_dataset(df):
    """
    Wrap a Pandas DataFrame using Great Expectations dataset API.

    Args:
        df: Pandas DataFrame to validate.
            Example: DataFrame with columns [track_id, track_name, artists, ...]

    Returns:
        Great Expectations PandasDataset wrapper.
            Example: <PandasDataset object with validation methods>
    """
    return PandasDataset(df)


# Run dataset-level expectations and return summary if all checks pass.
def validate_curated_parquet(parquet_uri, dataset):
    """Run all configured quality checks for a curated parquet dataset."""
    rules = rules_for_dataset(dataset)

    try:
        # Read parquet file(s) - simple approach
        df = pd.read_parquet(parquet_uri)
    except Exception as e:
        # Log the error for debugging and return failure
        error_msg = str(e)
        log.error(
            f"Failed to read parquet for {dataset} from {parquet_uri}: {error_msg}")
        # If schema merge fails or file not found, return failure gracefully
        if any(x in error_msg for x in ["incompatible types", "Unable to merge", "No such file", "NoSuchKey"]):
            return {"success": False, "checked_rows": 0, "dataset": dataset}
        # For other errors, still return gracefully with logging
        return {"success": False, "checked_rows": 0, "dataset": dataset, "error": error_msg}

    if df.empty:
        raise ValueError(f"{dataset} parquet output is empty")

    ge_df = as_ge_dataset(df)
    checks = []

    # Verify required columns exist
    for col in rules.get("required_columns", []):
        checks.append(ge_df.expect_column_to_exist(col))

    # Check null ratios
    for col, ratio in rules.get("max_null_ratio", {}).items():
        checks.append(ge_df.expect_column_values_to_not_be_null(
            col, mostly=1 - float(ratio)))

    # Validate numeric columns match pattern
    for col in rules.get("numeric_columns", []):
        checks.append(ge_df.expect_column_values_to_match_regex(
            col, r"^-?\d+(\.\d+)?$", mostly=0.95))

    # Validate timestamp columns
    date_pattern = r"^\d{4}-\d{2}-\d{2}( \d{2}:\d{2}:\d{2})?$"
    for col in rules.get("timestamp_columns", []):
        checks.append(ge_df.expect_column_values_to_match_regex(
            col, date_pattern, mostly=0.95))

    # Verify uniqueness for key columns
    for col in rules.get("unique_columns", []):
        checks.append(
            ge_df.expect_column_values_to_be_unique(col, mostly=0.995))

    if not all(check.get("success", False) for check in checks):
        raise ValueError(
            f"Quality checks failed for {dataset}: {len([c for c in checks if not c.get('success')])} checks failed")

    return {"success": True, "checked_rows": len(df), "dataset": dataset}
