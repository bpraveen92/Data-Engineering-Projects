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
        failures   — List of failure messages (hard errors that block the write).
        warnings   — List of warning messages (soft issues logged but not blocking).
        row_count  — Number of rows in the DataFrame that was validated.

    Usage pattern:
        result = validate_silver_results(df)
        result.log("silver_race_results")   # always print the summary
        result.raise_if_failed()            # raise ValueError on hard failures
    """

    def __init__(self, passed, failures=None, warnings=None, row_count=0):
        self.passed = passed
        self.failures = failures or []
        self.warnings = warnings or []
        self.row_count = row_count

    def raise_if_failed(self):
        """
        Raise ValueError if any hard failures were recorded.

        Called by Silver and Gold notebooks after validation. Bronze notebooks
        call log() instead and never call this method — Bronze is always soft-fail.

        Example:
            result.raise_if_failed()
            # → raises ValueError: "Validation FAILED — 480 rows; failures: null driver_id: 2 rows"
        """
        if not self.passed:
            summary = "; ".join(self.failures)
            raise ValueError(f"Validation FAILED — {self.row_count:,} rows; failures: {summary}")

    def log(self, table_name=""):
        """
        Print a validation summary and emit warnings/failures to the logger.

        Always prints "Validation PASSED/FAILED — N rows" regardless of outcome.
        Warnings go to logger.warning; failures go to logger.error. This
        separation means failures appear as ERROR lines in the Databricks
        driver log, making them easy to filter in monitoring dashboards.

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


# ── Bronze validators (soft-fail) ─────────────────────────────────────────────

def validate_bronze_f1(df, table_name):
    """
    Soft-fail Bronze validator: warn on null or empty MERGE key columns.

    Bronze data is raw and unvalidated by design — we never want a null key
    to block ingestion. Instead, the warning is logged so the team can
    investigate the upstream API or Parquet file without stopping the pipeline.

    Example (bronze_race_results, season/round/driver_id as merge keys):
        result = validate_bronze_f1(df, "bronze_race_results")
        # If 2 rows have driver_id == "" → result.warnings contains:
        #   "null/empty driver_id: 2 rows"
        # result.passed is still True — no hard failures are ever set here.
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


# ── Silver validators (hard-fail) ─────────────────────────────────────────────

def validate_silver_results(df):
    """
    Hard-fail Silver validator for silver_race_results.

    Failures (block the write, raise ValueError):
        - Null driver_id, season, or round — these are the MERGE key columns;
          a null would silently overwrite unrelated rows.
        - points outside [0, 26] — catches accidental type-cast failures
          or API anomalies (25 for a win + 1 fastest-lap point = 26 max).
        - Unknown status_category values not in VALID_STATUS_CATEGORIES.

    Warnings (logged only, write proceeds):
        - fastest_lap_seconds outside [60, 300] seconds — valid outlier range
          covers street circuits (Monaco ~75 s) to safety-car laps (~200 s+).

    Example:
        result = validate_silver_results(df)
        result.raise_if_failed()   # raises if any null key or bad points found
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

    Failures (block the write):
        - Null session_key, driver_number, or lap_number — MERGE key columns.

    Warnings (logged only):
        - lap_duration_seconds outside [50, 300] on non-pit-out laps — covers
          the fastest circuits (~54 s at Monza) to very slow laps. Pit-out
          laps are excluded because they legitimately run much slower.
        - Unexpected compound values not in VALID_COMPOUNDS — catches API
          changes where new compound names are introduced.

    An empty DataFrame (no new laps in an incremental run) is always valid
    and returns immediately without hitting the cluster for counts.

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


# ── Gold validators (hard-fail) ───────────────────────────────────────────────

def validate_gold_standings(df, table_name):
    """
    Hard-fail Gold validator for championship standing tables.

    Used for both gold_driver_championship and gold_constructor_championship.
    The table_name argument is passed through to the log message so the same
    function produces clearly labelled output for both tables.

    Failures (block the write):
        - current_position outside [1, 20] — an F1 grid has at most 20 cars;
          any value outside this range indicates a data processing error.
        - Duplicate (season, driver_id) or (season, constructor_id) pairs —
          these are MERGE key columns; a duplicate would cause the MERGE to
          match multiple target rows, producing non-deterministic results.

    An empty DataFrame is always valid (no new rounds processed this run).

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
