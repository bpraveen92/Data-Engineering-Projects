"""
Centralised schema definitions for all Bronze, Silver, and Gold Delta tables.

All Bronze schemas use StringType for raw API fields + two metadata columns.
Silver schemas use typed columns with derived fields.
Gold schemas reflect the final analytics shape.

merge_keys maps table name → list of columns that uniquely identify a row.
"""

from pyspark.sql.types import (
    BooleanType,
    DateType,
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

metadata_fields = [
    StructField("ingested_at",  TimestampType(), True),
    StructField("source_name",  StringType(),    True),
]

merge_keys = {
    # Bronze
    "bronze_race_schedule":          ["season", "round"],
    "bronze_race_results":           ["season", "round", "driver_id"],
    "bronze_qualifying":             ["season", "round", "driver_id"],
    "bronze_pit_stops":              ["season", "round", "driver_id", "stop_number"],
    "bronze_driver_standings":       ["season", "round", "driver_id"],
    "bronze_constructor_standings":  ["season", "round", "constructor_id"],
    "bronze_laps":                   ["session_key", "driver_number", "lap_number"],
    "bronze_stints":                 ["session_key", "driver_number", "stint_number"],
    # Silver
    "silver_race_results":           ["season", "round", "driver_id"],
    "silver_qualifying":             ["season", "round", "driver_id"],
    "silver_driver_standings":       ["season", "round", "driver_id"],
    "silver_constructor_standings":  ["season", "round", "constructor_id"],
    "silver_lap_analysis":           ["session_key", "driver_number", "lap_number"],
    # Gold
    "gold_driver_championship":      ["season", "driver_id"],
    "gold_constructor_championship": ["season", "constructor_id"],
    "gold_circuit_benchmarks":       ["circuit_id"],
    # Infrastructure
    "pipeline_checkpoints":          ["pipeline_name"],
}

bronze_race_schedule = StructType([
    StructField("season",       StringType(), True),
    StructField("round",        StringType(), True),
    StructField("race_name",    StringType(), True),
    StructField("circuit_id",   StringType(), True),
    StructField("circuit_name", StringType(), True),
    StructField("locality",     StringType(), True),
    StructField("country",      StringType(), True),
    StructField("lat",          StringType(), True),
    StructField("lon",          StringType(), True),
    StructField("race_date",    StringType(), True),
    StructField("race_time",    StringType(), True),
] + metadata_fields)

bronze_race_results = StructType([
    StructField("season",            StringType(), True),
    StructField("round",             StringType(), True),
    StructField("driver_id",         StringType(), True),
    StructField("driver_code",       StringType(), True),
    StructField("driver_number",     StringType(), True),
    StructField("constructor_id",    StringType(), True),
    StructField("grid_position",     StringType(), True),
    StructField("final_position",    StringType(), True),
    StructField("position_text",     StringType(), True),
    StructField("points",            StringType(), True),
    StructField("laps_completed",    StringType(), True),
    StructField("race_time",         StringType(), True),
    StructField("race_time_millis",  StringType(), True),
    StructField("fastest_lap_rank",  StringType(), True),
    StructField("fastest_lap_time",  StringType(), True),
    StructField("fastest_lap_speed", StringType(), True),
    StructField("status",            StringType(), True),
] + metadata_fields)

bronze_qualifying = StructType([
    StructField("season",              StringType(), True),
    StructField("round",               StringType(), True),
    StructField("driver_id",           StringType(), True),
    StructField("driver_code",         StringType(), True),
    StructField("constructor_id",      StringType(), True),
    StructField("qualifying_position", StringType(), True),
    StructField("q1_time",             StringType(), True),
    StructField("q2_time",             StringType(), True),
    StructField("q3_time",             StringType(), True),
] + metadata_fields)

bronze_pit_stops = StructType([
    StructField("season",          StringType(), True),
    StructField("round",           StringType(), True),
    StructField("driver_id",       StringType(), True),
    StructField("stop_number",     StringType(), True),
    StructField("lap",             StringType(), True),
    StructField("time_of_day",     StringType(), True),
    StructField("duration",        StringType(), True),
    StructField("duration_millis", StringType(), True),
] + metadata_fields)

bronze_driver_standings = StructType([
    StructField("season",         StringType(), True),
    StructField("round",          StringType(), True),
    StructField("driver_id",      StringType(), True),
    StructField("driver_code",    StringType(), True),
    StructField("constructor_id", StringType(), True),
    StructField("position",       StringType(), True),
    StructField("points",         StringType(), True),
    StructField("wins",           StringType(), True),
] + metadata_fields)

bronze_constructor_standings = StructType([
    StructField("season",           StringType(), True),
    StructField("round",            StringType(), True),
    StructField("constructor_id",   StringType(), True),
    StructField("constructor_name", StringType(), True),
    StructField("nationality",      StringType(), True),
    StructField("position",         StringType(), True),
    StructField("points",           StringType(), True),
    StructField("wins",             StringType(), True),
] + metadata_fields)

bronze_laps = StructType([
    StructField("session_key",       StringType(), True),
    StructField("driver_number",     StringType(), True),
    StructField("lap_number",        StringType(), True),
    StructField("lap_duration",      StringType(), True),
    StructField("is_pit_out_lap",    StringType(), True),
    StructField("date_start",        StringType(), True),
    StructField("duration_sector_1", StringType(), True),
    StructField("duration_sector_2", StringType(), True),
    StructField("duration_sector_3", StringType(), True),
    StructField("i1_speed",          StringType(), True),
    StructField("i2_speed",          StringType(), True),
    StructField("st_speed",          StringType(), True),
] + metadata_fields)

bronze_stints = StructType([
    StructField("session_key",       StringType(), True),
    StructField("driver_number",     StringType(), True),
    StructField("stint_number",      StringType(), True),
    StructField("lap_start",         StringType(), True),
    StructField("lap_end",           StringType(), True),
    StructField("compound",          StringType(), True),
    StructField("tyre_age_at_start", StringType(), True),
] + metadata_fields)

silver_race_results = StructType([
    StructField("season",                 IntegerType(), True),
    StructField("round",                  IntegerType(), True),
    StructField("circuit_id",             StringType(),  True),
    StructField("race_name",              StringType(),  True),
    StructField("race_date",              DateType(),    True),
    StructField("driver_id",              StringType(),  True),
    StructField("driver_code",            StringType(),  True),
    StructField("driver_number",          IntegerType(), True),
    StructField("constructor_id",         StringType(),  True),
    StructField("grid_position",          IntegerType(), True),
    StructField("final_position",         IntegerType(), True),
    StructField("is_classified",          BooleanType(), True),
    StructField("status",                 StringType(),  True),
    StructField("status_category",        StringType(),  True),
    StructField("points",                 DoubleType(),  True),
    StructField("laps_completed",         IntegerType(), True),
    StructField("race_time_seconds",      DoubleType(),  True),
    StructField("gap_to_winner_seconds",  DoubleType(),  True),
    StructField("fastest_lap_rank",       IntegerType(), True),
    StructField("fastest_lap_seconds",    DoubleType(),  True),
    StructField("fastest_lap_speed_kph",  DoubleType(),  True),
    StructField("ingested_at",           TimestampType(), True),
    StructField("source_name",                StringType(),  True),
])

silver_qualifying = StructType([
    StructField("season",                      IntegerType(), True),
    StructField("round",                       IntegerType(), True),
    StructField("circuit_id",                  StringType(),  True),
    StructField("race_date",                   DateType(),    True),
    StructField("driver_id",                   StringType(),  True),
    StructField("driver_code",                 StringType(),  True),
    StructField("constructor_id",              StringType(),  True),
    StructField("qualifying_position",         IntegerType(), True),
    StructField("q1_seconds",                  DoubleType(),  True),
    StructField("q2_seconds",                  DoubleType(),  True),
    StructField("q3_seconds",                  DoubleType(),  True),
    StructField("best_qualifying_time_seconds", DoubleType(), True),
    StructField("q_session_reached",           IntegerType(), True),
    StructField("gap_to_pole_seconds",         DoubleType(),  True),
    StructField("ingested_at",                TimestampType(), True),
    StructField("source_name",                     StringType(),  True),
])

silver_driver_standings = StructType([
    StructField("season",          IntegerType(), True),
    StructField("round",           IntegerType(), True),
    StructField("driver_id",       StringType(),  True),
    StructField("driver_code",     StringType(),  True),
    StructField("constructor_id",  StringType(),  True),
    StructField("position",        IntegerType(), True),
    StructField("points",          DoubleType(),  True),
    StructField("wins",            IntegerType(), True),
    StructField("position_change", IntegerType(), True),
    StructField("ingested_at",    TimestampType(), True),
    StructField("source_name",         StringType(),  True),
])

silver_constructor_standings = StructType([
    StructField("season",               IntegerType(), True),
    StructField("round",                IntegerType(), True),
    StructField("constructor_id",       StringType(),  True),
    StructField("constructor_name",     StringType(),  True),
    StructField("position",             IntegerType(), True),
    StructField("points",               DoubleType(),  True),
    StructField("wins",                 IntegerType(), True),
    StructField("points_gap_to_leader", DoubleType(),  True),
    StructField("ingested_at",         TimestampType(), True),
    StructField("source_name",              StringType(),  True),
])

silver_lap_analysis = StructType([
    StructField("session_key",       StringType(),  True),
    StructField("season",            IntegerType(), True),
    StructField("round",             IntegerType(), True),
    StructField("circuit_id",        StringType(),  True),
    StructField("driver_number",     IntegerType(), True),
    StructField("driver_id",         StringType(),  True),
    StructField("driver_code",       StringType(),  True),
    StructField("constructor_id",    StringType(),  True),
    StructField("lap_number",        IntegerType(), True),
    StructField("lap_duration_seconds", DoubleType(), True),
    StructField("is_pit_out_lap",    BooleanType(), True),
    StructField("sector_1_seconds",  DoubleType(),  True),
    StructField("sector_2_seconds",  DoubleType(),  True),
    StructField("sector_3_seconds",  DoubleType(),  True),
    StructField("i1_speed_kph",      DoubleType(),  True),
    StructField("i2_speed_kph",      DoubleType(),  True),
    StructField("st_speed_kph",      DoubleType(),  True),
    StructField("stint_number",      IntegerType(), True),
    StructField("compound",          StringType(),  True),
    StructField("tyre_age_laps",     IntegerType(), True),
    StructField("is_personal_best",  BooleanType(), True),
    StructField("ingested_at",      TimestampType(), True),
    StructField("source_name",           StringType(),  True),
])

gold_driver_championship = StructType([
    StructField("season",                   IntegerType(), True),
    StructField("driver_id",                StringType(),  True),
    StructField("driver_code",              StringType(),  True),
    StructField("constructor_id",           StringType(),  True),
    StructField("current_position",         IntegerType(), True),
    StructField("current_points",           DoubleType(),  True),
    StructField("wins",                     IntegerType(), True),
    StructField("podiums",                  IntegerType(), True),
    StructField("dnfs",                     IntegerType(), True),
    StructField("best_qualifying_position", IntegerType(), True),
    StructField("races_completed",          IntegerType(), True),
    StructField("last_round_processed",     IntegerType(), True),
    StructField("updated_at",              TimestampType(), True),
])

gold_constructor_championship = StructType([
    StructField("season",               IntegerType(), True),
    StructField("constructor_id",       StringType(),  True),
    StructField("constructor_name",     StringType(),  True),
    StructField("current_position",     IntegerType(), True),
    StructField("current_points",       DoubleType(),  True),
    StructField("wins",                 IntegerType(), True),
    StructField("podiums",              IntegerType(), True),
    StructField("races_completed",      IntegerType(), True),
    StructField("last_round_processed", IntegerType(), True),
    StructField("updated_at",          TimestampType(), True),
])

gold_circuit_benchmarks = StructType([
    StructField("circuit_id",                    StringType(),  True),
    StructField("circuit_name",                  StringType(),  True),
    StructField("country",                       StringType(),  True),
    StructField("all_time_fastest_lap_seconds",  DoubleType(),  True),
    StructField("fastest_lap_driver_id",         StringType(),  True),
    StructField("fastest_lap_constructor_id",    StringType(),  True),
    StructField("fastest_lap_season",            IntegerType(), True),
    StructField("fastest_lap_round",             IntegerType(), True),
    StructField("all_time_pole_seconds",         DoubleType(),  True),
    StructField("pole_driver_id",                StringType(),  True),
    StructField("pole_season",                   IntegerType(), True),
    StructField("pole_round",                    IntegerType(), True),
    StructField("total_races_held",              IntegerType(), True),
    StructField("last_race_winner_id",           StringType(),  True),
    StructField("last_race_season",              IntegerType(), True),
    StructField("last_race_round",               IntegerType(), True),
    StructField("updated_at",                   TimestampType(), True),
])

gold_tyre_strategy_report = StructType([
    StructField("season",                IntegerType(), True),
    StructField("round",                 IntegerType(), True),
    StructField("circuit_id",            StringType(),  True),
    StructField("race_date",             DateType(),    True),
    StructField("driver_id",             StringType(),  True),
    StructField("driver_code",           StringType(),  True),
    StructField("constructor_id",        StringType(),  True),
    StructField("final_position",        IntegerType(), True),
    StructField("total_stints",          IntegerType(), True),
    StructField("compounds_used",        StringType(),  True),
    StructField("stint_1_compound",      StringType(),  True),
    StructField("stint_1_laps",          IntegerType(), True),
    StructField("stint_2_compound",      StringType(),  True),
    StructField("stint_2_laps",          IntegerType(), True),
    StructField("stint_3_compound",      StringType(),  True),
    StructField("stint_3_laps",          IntegerType(), True),
    StructField("total_pit_time_seconds", DoubleType(), True),
    StructField("pit_count",             IntegerType(), True),
    StructField("processed_at",         TimestampType(), True),
])

pipeline_checkpoints = StructType([
    StructField("pipeline_name",           StringType(),    False),
    StructField("last_processed_version",  LongType(),      True),
    StructField("records_processed",       LongType(),      True),
    StructField("processed_at",            TimestampType(), True),
])

table_schemas = {
    "bronze_race_schedule":          bronze_race_schedule,
    "bronze_race_results":           bronze_race_results,
    "bronze_qualifying":             bronze_qualifying,
    "bronze_pit_stops":              bronze_pit_stops,
    "bronze_driver_standings":       bronze_driver_standings,
    "bronze_constructor_standings":  bronze_constructor_standings,
    "bronze_laps":                   bronze_laps,
    "bronze_stints":                 bronze_stints,
    "silver_race_results":           silver_race_results,
    "silver_qualifying":             silver_qualifying,
    "silver_driver_standings":       silver_driver_standings,
    "silver_constructor_standings":  silver_constructor_standings,
    "silver_lap_analysis":           silver_lap_analysis,
    "gold_driver_championship":      gold_driver_championship,
    "gold_constructor_championship": gold_constructor_championship,
    "gold_circuit_benchmarks":       gold_circuit_benchmarks,
    "gold_tyre_strategy_report":     gold_tyre_strategy_report,
    "pipeline_checkpoints":          pipeline_checkpoints,
}
