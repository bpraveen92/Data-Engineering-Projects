"""
Delta Lake helpers: MERGE, CDF reads, checkpoints, Liquid Clustering.

All functions accept a fully-qualified table name (catalog.schema.table)
so they work unchanged across dev and prod environments.
"""

import logging
from datetime import datetime, timezone

import pyspark.sql.functions as F
from pyspark.sql import DataFrame, SparkSession
from delta.tables import DeltaTable

logger = logging.getLogger(__name__)


# ── Catalog / Schema ───────────────────────────────────────────────────────────

def get_or_create_catalog_schema(spark, catalog, schema):
    """
    Create the Unity Catalog catalog and schema if they do not already exist,
    then set them as the active catalog and schema for the session.

    Called at the top of every ETL notebook so subsequent table references
    can use unqualified names where needed.

    Example:
        get_or_create_catalog_schema(spark, "f1_intelligence", "f1_dev")
        # → runs: CREATE CATALOG IF NOT EXISTS `f1_intelligence`
        #          CREATE SCHEMA  IF NOT EXISTS `f1_intelligence`.`f1_dev`
        #          USE CATALOG `f1_intelligence`
        #          USE SCHEMA  `f1_dev`
    """
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    spark.sql(f"USE CATALOG `{catalog}`")
    spark.sql(f"USE SCHEMA `{schema}`")
    logger.info("Using %s.%s", catalog, schema)


def table_exists(spark, full_table_name):
    """
    Return True if a Delta table exists in the Unity Catalog, False otherwise.

    Uses DESCRIBE TABLE rather than querying information_schema so it works
    consistently across Databricks Runtime versions.

    Example:
        table_exists(spark, "f1_intelligence.f1_dev.bronze_race_results")
        # → True  (if the table has been created)
        # → False (on first run before Bronze ingestion)
    """
    try:
        spark.sql(f"DESCRIBE TABLE {full_table_name}")
        return True
    except Exception:
        return False


# ── Delta table features ───────────────────────────────────────────────────────

def enable_cdf(spark, full_table_name):
    """
    Enable Delta Change Data Feed on an existing table.

    CDF allows downstream Silver and Gold notebooks to read only the rows
    that changed since the last checkpoint version, avoiding full-table scans
    on every incremental run. Called once by merge_delta() at table creation.

    Example:
        enable_cdf(spark, "f1_intelligence.f1_dev.bronze_race_results")
        # → ALTER TABLE ... SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')
    """
    spark.sql(
        f"ALTER TABLE {full_table_name} "
        "SET TBLPROPERTIES ('delta.enableChangeDataFeed' = 'true')"
    )
    logger.info("CDF enabled on %s", full_table_name)


def enable_liquid_clustering(spark, full_table_name, cluster_cols):
    """
    Apply Liquid Clustering to an existing Delta table.

    Liquid Clustering replaces static PARTITIONED BY + ZORDER and lets
    Databricks reorganise data files automatically at cluster time. It is
    applied once after table creation, using the MERGE keys as cluster columns
    so that MERGE predicates can skip irrelevant files efficiently.

    Example:
        enable_liquid_clustering(
            spark,
            "f1_intelligence.f1_dev.bronze_race_results",
            ["season", "round", "driver_id"],
        )
        # → ALTER TABLE ... CLUSTER BY (season, round, driver_id)
    """
    cols = ", ".join(cluster_cols)
    spark.sql(f"ALTER TABLE {full_table_name} CLUSTER BY ({cols})")
    logger.info("Liquid clustering set on %s by (%s)", full_table_name, cols)


# ── MERGE / write ──────────────────────────────────────────────────────────────

def merge_delta(spark, df, full_table_name, merge_keys):
    """
    Upsert a DataFrame into a Delta table using a MERGE (insert-or-update).

    First call: the table does not yet exist, so it is created via an
    overwrite write, then CDF and Liquid Clustering are enabled immediately.

    Subsequent calls: a true Delta MERGE is executed —
        - Matched rows (same merge key values) → all non-key columns updated.
        - Unmatched source rows                → inserted as new rows.

    This is the core function that makes every pipeline run fully idempotent.
    Re-running for the same season/round replaces existing rows rather than
    duplicating them, and post-race steward corrections propagate automatically.

    Example:
        merge_delta(
            spark, df_results,
            "f1_intelligence.f1_dev.bronze_race_results",
            ["season", "round", "driver_id"],
        )
        # First run  → creates table, enables CDF + Liquid Clustering
        # Subsequent → MERGE ON target.season=source.season
        #                       AND target.round=source.round
        #                       AND target.driver_id=source.driver_id
    """
    if not table_exists(spark, full_table_name):
        logger.info("Creating %s (first write)", full_table_name)
        df.write.format("delta").mode("overwrite").saveAsTable(full_table_name)
        enable_cdf(spark, full_table_name)
        enable_liquid_clustering(spark, full_table_name, merge_keys)
        return

    cond = " AND ".join(f"target.{k} = source.{k}" for k in merge_keys)
    all_cols = df.columns

    (
        DeltaTable.forName(spark, full_table_name).alias("target")
        .merge(df.alias("source"), cond)
        .whenMatchedUpdate(set={c: f"source.{c}" for c in all_cols if c not in merge_keys})
        .whenNotMatchedInsertAll()
        .execute()
    )
    logger.info("MERGE complete on %s", full_table_name)


def write_delta_append(df, full_table_name):
    """
    Append rows to an existing Delta table without deduplication.

    Used for gold_tyre_strategy_report, which accumulates one set of rows per
    race per run. Idempotency is enforced upstream via the CDF checkpoint guard
    — if no new Silver rows exist the Gold notebook exits before reaching this
    call, so duplicate appends cannot occur.

    Example:
        write_delta_append(df_tyre, "f1_intelligence.f1_dev.gold_tyre_strategy_report")
    """
    df.write.format("delta").mode("append").saveAsTable(full_table_name)
    logger.info("APPEND complete on %s", full_table_name)


# ── Table version ──────────────────────────────────────────────────────────────

def get_current_table_version(spark, full_table_name):
    """
    Return the latest commit version number of a Delta table.

    Used by read_incremental_or_full() to record which version was processed
    and by save_checkpoint() to stamp the checkpoint record.

    Example: get_current_table_version(spark, "f1_intelligence.f1_dev.bronze_race_results")
             → 5  (after 6 commits: initial write + 5 subsequent MERGEs)
    """
    return spark.sql(f"DESCRIBE HISTORY {full_table_name} LIMIT 1").collect()[0]["version"]


# ── CDF reads ─────────────────────────────────────────────────────────────────

def read_cdf_changes(spark, full_table_name, from_version):
    """
    Read only the rows that changed in a Delta table since a given version.

    Filters to _change_type IN ('insert', 'update_postimage') so the caller
    always receives the final state of each changed record — not the before
    image. This ensures steward penalty corrections (which arrive as
    update_postimage rows) flow correctly through Bronze → Silver → Gold.

    CDF metadata columns (_change_type, _commit_version, _commit_timestamp)
    are dropped before returning so the DataFrame schema matches a normal read.

    Example:
        read_cdf_changes(spark, "f1_intelligence.f1_dev.bronze_race_results", from_version=3)
        # → DataFrame of inserts and post-update rows committed at versions 4, 5, 6, ...
    """
    return (
        spark.read
        .format("delta")
        .option("readChangeFeed", "true")
        .option("startingVersion", from_version)
        .table(full_table_name)
        .filter(F.col("_change_type").isin("insert", "update_postimage"))
        .drop("_change_type", "_commit_version", "_commit_timestamp")
    )


def read_incremental_or_full(spark, full_table_name, checkpoint_table, pipeline_name):
    """
    Return a (df, source_version) tuple for either a full or incremental read.

    Decision logic:
      - No checkpoint found (first run)  → full table read; source_version is
        the current table version.
      - Checkpoint exists, no new commits → returns an empty DataFrame with the
        correct schema; source_version is the current (unchanged) version.
      - Checkpoint exists, new commits    → CDF read from last_version+1 up to
        the current version via read_cdf_changes().

    The caller is responsible for calling save_checkpoint() with source_version
    after a successful downstream write.

    Example:
        df, version = read_incremental_or_full(
            spark,
            "f1_intelligence.f1_dev.bronze_race_results",
            "f1_intelligence.f1_dev.pipeline_checkpoints",
            "bronze_to_silver_results",
        )
        # First run  → full read of bronze_race_results, version=0
        # Second run → CDF read from version 1 → N (new/changed rows only)
    """
    last_version = get_latest_checkpoint_version(spark, checkpoint_table, pipeline_name)

    if last_version is None:
        logger.info("[%s] First run — full read of %s", pipeline_name, full_table_name)
        df = spark.read.format("delta").table(full_table_name)
        return df, get_current_table_version(spark, full_table_name)

    current_version = get_current_table_version(spark, full_table_name)
    if current_version <= last_version:
        logger.info("[%s] No new data — table version %d matches checkpoint", pipeline_name, current_version)
        empty_df = spark.createDataFrame([], schema=spark.read.format("delta").table(full_table_name).schema)
        return empty_df, current_version

    logger.info("[%s] Incremental CDF read %s v%d → v%d", pipeline_name, full_table_name, last_version + 1, current_version)
    return read_cdf_changes(spark, full_table_name, last_version + 1), current_version


# ── Checkpoints ───────────────────────────────────────────────────────────────

def save_checkpoint(spark, checkpoint_table, pipeline_name, processed_version, records_processed):
    """
    Persist a pipeline checkpoint so the next run knows where to resume from.

    Upserts a single row into the pipeline_checkpoints table keyed on
    pipeline_name. Three checkpoint entries are maintained across the project:
        "bronze_to_silver_results" — tracks bronze_race_results table version
        "bronze_to_silver_laps"    — tracks bronze_laps table version
        "silver_to_gold"           — tracks silver_race_results table version

    Example:
        save_checkpoint(
            spark, "f1_intelligence.f1_dev.pipeline_checkpoints",
            pipeline_name="bronze_to_silver_results",
            processed_version=5,
            records_processed=480,
        )
        # pipeline_checkpoints row: pipeline_name="bronze_to_silver_results",
        #   last_processed_version=5, records_processed=480, processed_at=<now>
    """
    from utils.schema import PIPELINE_CHECKPOINTS
    now = datetime.now(timezone.utc)
    df = spark.createDataFrame(
        [(pipeline_name, processed_version, records_processed, now)],
        schema=PIPELINE_CHECKPOINTS,
    )
    merge_delta(spark, df, checkpoint_table, ["pipeline_name"])
    logger.info("[%s] Checkpoint saved at version %d (%d records)", pipeline_name, processed_version, records_processed)


def get_latest_checkpoint_version(spark, checkpoint_table, pipeline_name):
    """
    Look up the last successfully processed Delta version for a pipeline.

    Returns None when the checkpoint table does not yet exist (first run),
    which signals read_incremental_or_full() to perform a full table read.

    Example:
        get_latest_checkpoint_version(
            spark,
            "f1_intelligence.f1_dev.pipeline_checkpoints",
            "bronze_to_silver_results",
        )
        # First run  → None  (table doesn't exist yet)
        # Second run → 5     (version recorded after the first successful write)
    """
    if not table_exists(spark, checkpoint_table):
        return None
    rows = (
        spark.read.format("delta").table(checkpoint_table)
        .filter(F.col("pipeline_name") == pipeline_name)
        .select("last_processed_version")
        .collect()
    )
    return int(rows[0]["last_processed_version"]) if rows else None


# ── Metadata columns ──────────────────────────────────────────────────────────

def add_metadata_columns(df, source):
    """
    Append ingested_at and source_name columns to a Bronze DataFrame.

    ingested_at — current cluster timestamp at the time the notebook cell runs.
    source_name — identifies the originating API ("jolpica_f1" or "openf1"),
                  useful for lineage tracing and debugging schema mismatches.

    Example:
        df = add_metadata_columns(spark.read.parquet(path), "jolpica_f1")
        # df now has two extra columns:
        #   ingested_at: 2024-03-02 16:00:00.000 UTC
        #   source_name: "jolpica_f1"
    """
    return (
        df
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_name", F.lit(source))
    )


# ── Diagnostics ───────────────────────────────────────────────────────────────

def print_table_stats(spark, full_table_name):
    """
    Print a one-line summary of a Delta table's current row count and version.

    Called after every MERGE or APPEND as a lightweight observability check.
    Output format: "<catalog.schema.table>: N rows, Delta version V"

    Example output:
        f1_intelligence.f1_dev.bronze_race_results: 480 rows, Delta version 1
    """
    count = spark.read.format("delta").table(full_table_name).count()
    version = get_current_table_version(spark, full_table_name)
    print(f"{full_table_name}: {count:,} rows, Delta version {version}")
