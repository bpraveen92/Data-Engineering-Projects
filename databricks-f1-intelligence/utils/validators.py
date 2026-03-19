"""
Data quality validators for each pipeline layer.

Bronze validators are soft-fail (log warnings only).
Silver and Gold validators are hard-fail (raise ValueError on failures).
"""

import logging

import pyspark.sql.functions as F

logger = logging.getLogger(__name__)

VALID_COMPOUNDS = {"SOFT", "MEDIUM", "HARD", "INTERMEDIATE", "WET", "UNKNOWN", ""}
VALID_STATUS_CATEGORIES = {
    "FINISHED", "RETIRED_MECHANICAL", "RETIRED_ACCIDENT",
    "DISQUALIFIED", "DID_NOT_START", "OTHER",
}

class ValidationResult:
    """
    Carries the outcome of a data quality check for one table.

    Attributes:
        passed     — True if no hard failures were recorded.
        failures   — Hard errors that block the write.
        warnings   — Soft issues logged but not blocking.
        row_count  — Row count of the validated DataFrame.

    Usage:
        result = validate_silver_results(df)
        result.log("silver_race_results")
        result.raise_if_failed()
    """

    def __init__(self, passed, failures=None, warnings=None, row_count=0):
        self.passed = passed
        self.failures = failures or []
        self.warnings = warnings or []
        self.row_count = row_count

    def raise_if_failed(self):
        """
        Raise ValueError if any hard failures exist.

        Example:
            result.raise_if_failed()
            # → ValueError: "Validation FAILED — 480 rows; failures: null driver_id: 2 rows"
        """
        if not self.passed:
            summary = "; ".join(self.failures)
            raise ValueError(f"Validation FAILED — {self.row_count:,} rows; failures: {summary}")

    def log(self, table_name=""):
        """
        Print validation summary; emit warnings and failures to the logger.

        Example output:
            Validation PASSED — 480 rows
            WARNING [bronze_race_results] null/empty driver_id: 2 rows
        """
        prefix = f"[{table_name}] " if table_name else ""
        status = "PASSED" if self.passed else "FAILED"
        print(f"Validation {status} — {self.row_count:,} rows")
        for w in self.warnings:
            logger.warning("%s%s", prefix, w)
        for f in self.failures:
            logger.error("%sFAIL: %s", prefix, f)

def validate_bronze_f1(df, table_name):
    """
    Soft-fail Bronze validator: warns on null/empty MERGE key columns, never blocks ingestion.

    Example:
        result = validate_bronze_f1(df, "bronze_race_results")
        # 2 rows with driver_id == "" → result.warnings: ["null/empty driver_id: 2 rows"]
        # result.passed is always True
    """
    from utils.schema import MERGE_KEYS
    result = ValidationResult(passed=True, row_count=df.count())
    for key in MERGE_KEYS.get(table_name, []):
        if key not in df.columns:
            continue
        null_count = df.filter(F.col(key).isNull() | (F.col(key) == "")).count()
        if null_count > 0:
            result.warnings.append(f"null/empty {key}: {null_count:,} rows")
    result.log(table_name)
    return result

def validate_silver_results(df):
    """
    Hard-fail Silver validator for silver_race_results.

    Failures: null driver_id/season/round; points outside [0, 26]; unknown status_category.
    Warnings: fastest_lap_seconds outside [60, 300].

    Example:
        result = validate_silver_results(df)
        result.raise_if_failed()
    """
    failures = []
    warnings = []
    row_count = df.count()

    for col_name in ("driver_id", "season", "round"):
        nulls = df.filter(F.col(col_name).isNull()).count()
        if nulls > 0:
            failures.append(f"null {col_name}: {nulls:,} rows")

    if "points" in df.columns:
        bad_points = df.filter(F.col("points").isNotNull() & ~F.col("points").between(0, 26)).count()
        if bad_points > 0:
            failures.append(f"points out of range [0, 26]: {bad_points:,} rows")

    if "fastest_lap_seconds" in df.columns:
        bad_laps = df.filter(F.col("fastest_lap_seconds").isNotNull() & ~F.col("fastest_lap_seconds").between(60, 300)).count()
        if bad_laps > 0:
            warnings.append(f"fastest_lap_seconds outside [60, 300]: {bad_laps:,} rows")

    if "status_category" in df.columns:
        bad_status = df.filter(~F.col("status_category").isin(*VALID_STATUS_CATEGORIES)).count()
        if bad_status > 0:
            failures.append(f"unknown status_category: {bad_status:,} rows")

    result = ValidationResult(passed=len(failures) == 0, failures=failures, warnings=warnings, row_count=row_count)
    result.log("silver_race_results")
    return result

def validate_silver_laps(df):
    """
    Hard-fail Silver validator for silver_lap_analysis.

    Failures: null session_key/driver_number/lap_number.
    Warnings: non-pit-out lap_duration_seconds outside [50, 300]; unexpected compound values.
    Empty DataFrame is always valid (no new laps in an incremental run).

    Example:
        result = validate_silver_laps(df_final_laps)
        result.raise_if_failed()
    """
    failures = []
    warnings = []
    row_count = df.count()

    if row_count == 0:
        result = ValidationResult(passed=True, row_count=0)
        result.log("silver_lap_analysis")
        return result

    for col_name in ("session_key", "driver_number", "lap_number"):
        if col_name not in df.columns:
            continue
        nulls = df.filter(F.col(col_name).isNull()).count()
        if nulls > 0:
            failures.append(f"null {col_name}: {nulls:,} rows")

    if "lap_duration_seconds" in df.columns:
        bad_duration = df.filter(
            F.col("lap_duration_seconds").isNotNull() &
            F.col("is_pit_out_lap").isNotNull() &
            ~F.col("is_pit_out_lap") &
            ~F.col("lap_duration_seconds").between(50, 300)
        ).count()
        if bad_duration > 0:
            warnings.append(f"lap_duration_seconds outside [50, 300] for non-pit-out laps: {bad_duration:,} rows")

    if "compound" in df.columns:
        bad_compound = df.filter(F.col("compound").isNotNull() & ~F.col("compound").isin(*VALID_COMPOUNDS)).count()
        if bad_compound > 0:
            warnings.append(f"unexpected compound values: {bad_compound:,} rows")

    result = ValidationResult(passed=len(failures) == 0, failures=failures, warnings=warnings, row_count=row_count)
    result.log("silver_lap_analysis")
    return result

def validate_gold_standings(df, table_name):
    """
    Hard-fail Gold validator for championship standing tables (driver and constructor).

    Failures: current_position outside [1, 25]; duplicate (season, driver_id/constructor_id).
    Empty DataFrame is always valid.

    Example:
        result = validate_gold_standings(df_driver_champ, "gold_driver_championship")
        result.raise_if_failed()
    """
    failures = []
    row_count = df.count()

    if row_count == 0:
        return ValidationResult(passed=True, row_count=0)

    if "current_position" in df.columns:
        bad_pos = df.filter(F.col("current_position").isNotNull() & ~F.col("current_position").between(1, 25)).count()
        if bad_pos > 0:
            failures.append(f"current_position outside [1, 25]: {bad_pos:,} rows")

    key_col = "driver_id" if "driver_id" in df.columns else "constructor_id"
    if "season" in df.columns and key_col in df.columns:
        dupes = df.groupBy("season", key_col).count().filter(F.col("count") > 1).count()
        if dupes > 0:
            failures.append(f"duplicate (season, {key_col}) pairs: {dupes:,}")

    result = ValidationResult(passed=len(failures) == 0, failures=failures, row_count=row_count)
    result.log(table_name)
    return result
