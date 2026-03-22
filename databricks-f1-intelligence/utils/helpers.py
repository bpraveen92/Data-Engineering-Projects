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

def get_or_create_catalog_schema(spark, catalog, schema):
    """
    Create catalog and schema if they don't exist, then set them as active.
    Called at the top of every ETL notebook.

    Example:
        get_or_create_catalog_schema(spark, "f1_intelligence", "f1_dev")
        # → CREATE CATALOG IF NOT EXISTS `f1_intelligence`
        #   CREATE SCHEMA  IF NOT EXISTS `f1_intelligence`.`f1_dev`
        #   USE CATALOG `f1_intelligence` / USE SCHEMA `f1_dev`
    """
    spark.sql(f"CREATE CATALOG IF NOT EXISTS `{catalog}`")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS `{catalog}`.`{schema}`")
    spark.sql(f"USE CATALOG `{catalog}`")
    spark.sql(f"USE SCHEMA `{schema}`")
    logger.info("Using %s.%s", catalog, schema)

def table_exists(spark, full_table_name):
    """
    Return True if a Delta table exists in Unity Catalog, False otherwise.
    Uses DESCRIBE TABLE — works consistently across all Databricks Runtime versions.

    Example:
        table_exists(spark, "f1_intelligence.f1_dev.bronze_race_results")
        # → True  (table exists)
        # → False (first run, before Bronze ingestion)
    """
    try:
        spark.sql(f"DESCRIBE TABLE {full_table_name}")
        return True
    except Exception:
        return False

def enable_cdf(spark, full_table_name):
    """
    Enable Delta Change Data Feed on an existing table.

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

    Example:
        enable_liquid_clustering(spark, "f1_intelligence.f1_dev.bronze_race_results",
                                 ["season", "round", "driver_id"])
        # → ALTER TABLE ... CLUSTER BY (season, round, driver_id)
    """
    cols = ", ".join(cluster_cols)
    spark.sql(f"ALTER TABLE {full_table_name} CLUSTER BY ({cols})")
    logger.info("Liquid clustering set on %s by (%s)", full_table_name, cols)

def merge_delta(spark, df, full_table_name, merge_keys):
    """
    Upsert a DataFrame into a Delta table.

    First call: creates the table, then enables CDF and Liquid Clustering.
    Subsequent calls: MERGE ON merge_keys — matched rows updated, new rows inserted.
    Source rows are deduplicated by merge_keys before every write.

    Example:
        merge_delta(spark, df_results, "f1_intelligence.f1_dev.bronze_race_results",
                    ["season", "round", "driver_id"])
        # First run  → creates table, enables CDF + Liquid Clustering
        # Subsequent → MERGE ON target.season=source.season AND ...
    """
    # I'm deduplicating on merge keys before the MERGE to prevent Delta ambiguity —
    # range-joins and CDF batches can both produce multiple rows for the same key.
    df = df.dropDuplicates(merge_keys)

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
    Append rows to a Delta table without deduplication.
    Idempotency is enforced upstream by the CDF checkpoint guard.

    Example:
        write_delta_append(df_tyre, "f1_intelligence.f1_dev.gold_tyre_strategy_report")
    """
    df.write.format("delta").mode("append").saveAsTable(full_table_name)
    logger.info("APPEND complete on %s", full_table_name)

def get_current_table_version(spark, full_table_name):
    """
    Return the latest commit version number of a Delta table.

    Example: get_current_table_version(spark, "f1_intelligence.f1_dev.bronze_race_results")
             → 5  (after 6 commits: initial write + 5 subsequent MERGEs)
    """
    return spark.sql(f"DESCRIBE HISTORY {full_table_name} LIMIT 1").collect()[0]["version"]

def read_cdf_changes(spark, full_table_name, from_version):
    """
    Read only changed rows since a given version, filtered to inserts and
    post-update images (ensures steward corrections propagate correctly).
    CDF metadata columns are dropped before returning.

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
    Return (df, source_version) for either a full or incremental read.

    - No checkpoint (first run)  → full read; source_version = current version.
    - No new commits             → empty DataFrame; source_version = current version.
    - New commits exist          → CDF read from last_version+1 to current version.

    Example:
        df, version = read_incremental_or_full(
            spark, "f1_intelligence.f1_dev.bronze_race_results",
            "f1_intelligence.f1_dev.pipeline_checkpoints", "bronze_to_silver_results")
        # First run  → full read, version=0
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

def save_checkpoint(spark, checkpoint_table, pipeline_name, processed_version, records_processed):
    """
    Upsert a checkpoint row keyed on pipeline_name.
    Three entries exist: "bronze_to_silver_results", "bronze_to_silver_laps", "silver_to_gold".

    Example:
        save_checkpoint(spark, "f1_intelligence.f1_dev.pipeline_checkpoints",
                        "bronze_to_silver_results", processed_version=5, records_processed=480)
        # → pipeline_checkpoints row: pipeline_name="bronze_to_silver_results",
        #   last_processed_version=5, records_processed=480, processed_at=<now>
    """
    from utils.schema import pipeline_checkpoints
    now = datetime.now(timezone.utc)
    df = spark.createDataFrame(
        [(pipeline_name, processed_version, records_processed, now)],
        schema=pipeline_checkpoints,
    )
    merge_delta(spark, df, checkpoint_table, ["pipeline_name"])
    logger.info("[%s] Checkpoint saved at version %d (%d records)", pipeline_name, processed_version, records_processed)

def get_latest_checkpoint_version(spark, checkpoint_table, pipeline_name):
    """
    Return the last successfully processed Delta version for a pipeline, or None on first run.

    Example:
        get_latest_checkpoint_version(spark, "f1_intelligence.f1_dev.pipeline_checkpoints",
                                      "bronze_to_silver_results")
        # First run  → None
        # Second run → 5
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

def add_metadata_columns(df, source):
    """
    Append ingested_at (current timestamp) and source_name columns.

    Example:
        df = add_metadata_columns(spark.read.parquet(path), "jolpica_f1")
        # df has two extra columns: ingested_at, source_name="jolpica_f1"
    """
    return (
        df
        .withColumn("ingested_at", F.current_timestamp())
        .withColumn("source_name", F.lit(source))
    )

def print_table_stats(spark, full_table_name):
    """
    Print a one-line summary of a Delta table's current row count and version.

    Example output:
        f1_intelligence.f1_dev.bronze_race_results: 480 rows, Delta version 1
    """
    count = spark.read.format("delta").table(full_table_name).count()
    version = get_current_table_version(spark, full_table_name)
    print(f"{full_table_name}: {count:,} rows, Delta version {version}")
