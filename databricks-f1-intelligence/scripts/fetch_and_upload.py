"""
Fetch F1 data locally and upload to Databricks Workspace.

Community Edition executors and driver cannot reach external APIs, so all data
is fetched here locally, saved as Parquet, and uploaded to the workspace.
The Bronze notebook then reads from the workspace path instead of the API.

Supports resume: if a previous run was interrupted, the script skips already-
fetched (season, round) pairs by reading data/fetch_progress.json.

Usage:
    # Full 2024 season
    python3 scripts/fetch_and_upload.py --season 2024 --mode full

    # Single 2025 round
    python3 scripts/fetch_and_upload.py --season 2025 --round 1 --mode incremental

    # Clear progress cache and re-fetch everything
    python3 scripts/fetch_and_upload.py --season 2024 --mode full --reset-progress
"""

import argparse
import json
import logging
import os
import subprocess
import sys

import pandas as pd

PROJECT_DIR = os.path.dirname(os.path.dirname(__file__))
UTILS_DIR   = os.path.join(PROJECT_DIR, "utils")
# Add utils/ directly so jolpica and openf1 are importable as standalone modules,
# which avoids triggering utils/__init__.py (which imports delta — Databricks-only).
sys.path.insert(0, UTILS_DIR)
from jolpica import JolpicaClient
from openf1 import OpenF1Client

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-7s  %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger(__name__)

PROJECT_ROOT      = os.path.dirname(os.path.dirname(__file__))
LOCAL_DATA_DIR    = os.path.join(PROJECT_ROOT, "data")
PROGRESS_FILE     = os.path.join(LOCAL_DATA_DIR, "fetch_progress.json")
WORKSPACE_DATA    = "/Workspace/Users/pravbala30@protonmail.com/.bundle/f1-intelligence/dev/f1_data"


# ── Progress tracking (resume support) ────────────────────────────────────────

def load_progress():
    """
    Load the fetch progress cache from disk.

    Returns a dict with two top-level keys — "jolpica" and "openf1" — each
    mapping completed fetch keys to their row counts.

    Example return value (after fetching 2024 Round 1 results):
        {
            "jolpica": {"races_2024": 24, "2024_r1_results": 20},
            "openf1":  {"sessions_2024": 24, "stints_9158": 60}
        }

    Returns an empty skeleton dict when no progress file exists (first run).
    """
    if os.path.exists(PROGRESS_FILE):
        with open(PROGRESS_FILE) as f:
            return json.load(f)
    return {"jolpica": {}, "openf1": {}}


def save_progress(progress):
    """Flush the in-memory progress dict to fetch_progress.json on disk."""
    with open(PROGRESS_FILE, "w") as f:
        json.dump(progress, f, indent=2)


def is_done(progress, source, key):
    """
    Return True if a fetch task has already been completed in a prior run.

    Example: is_done(progress, "jolpica", "2024_r1_results") → True
    """
    return key in progress.get(source, {})


def mark_done(progress, source, key, row_count):
    """
    Record a completed fetch in the in-memory progress dict and flush to disk.

    Keys follow the pattern:
        Jolpica  — "races_2024", "2024_r3_results", "2024_r3_qualifying", ...
        OpenF1   — "sessions_2024", "stints_9158", "laps_9158_d44", ...

    Example:
        mark_done(progress, "jolpica", "2024_r1_results", 20)
        # progress["jolpica"]["2024_r1_results"] == 20
        # fetch_progress.json updated on disk
    """
    progress.setdefault(source, {})[key] = row_count
    save_progress(progress)


# ── Workspace upload ───────────────────────────────────────────────────────────

def upload_to_workspace(local_path, workspace_path):
    """
    Upload a local file to the Databricks Workspace via the CLI.

    Uses `databricks workspace import --format RAW --overwrite` so subsequent
    runs always replace the file with the latest fetch.

    Example:
        upload_to_workspace(
            "data/bronze_race_results.parquet",
            "/Workspace/Users/pravbala30@protonmail.com/.bundle/f1-intelligence/dev/f1_data/bronze_race_results.parquet"
        )

    Raises RuntimeError if the CLI exits with a non-zero return code.
    """
    filename = os.path.basename(local_path)
    logger.info("Uploading %s → %s", filename, workspace_path)
    result = subprocess.run(
        [
            "databricks", "workspace", "import",
            "--file", local_path,
            "--format", "RAW",
            "--overwrite",
            workspace_path,
        ],
        capture_output=True,
        text=True,
    )
    if result.returncode != 0:
        raise RuntimeError(f"Upload failed for {filename}:\n{result.stderr}")
    logger.info("  Uploaded successfully")


def ensure_workspace_dir():
    """
    Create the workspace target directory if it does not already exist.

    Uses `databricks workspace mkdirs` which is idempotent — a warning is
    logged if the call fails (e.g. directory already exists) but execution
    continues, since upload_to_workspace will catch a genuine permission error.
    """
    logger.info("Ensuring workspace directory exists: %s", WORKSPACE_DATA)
    result = subprocess.run(
        ["databricks", "workspace", "mkdirs", WORKSPACE_DATA],
        capture_output=True, text=True,
    )
    if result.returncode != 0:
        logger.warning("mkdirs: %s", result.stderr.strip())


# ── Jolpica fetch ──────────────────────────────────────────────────────────────

def fetch_jolpica(season, rounds):
    """
    Fetch all Jolpica-F1 tables for the given season and round list.

    Always re-fetches all data on every run — Jolpica is ~144 calls at 1 s
    delay (~2.5 min total), which is fast enough that resume tracking adds
    more complexity than it saves. Resume tracking is reserved for the much
    larger OpenF1 lap fetch (~480 calls).

    Args:
        season — e.g. 2024
        rounds — e.g. [1, 2, 3] for the first three rounds, or range(1, 25)

    Returns a dict of DataFrames keyed by table name:
        {
            "race_schedule":         DataFrame(24 rows for 2024),
            "race_results":          DataFrame(~480 rows  — 24 rounds × 20 drivers),
            "qualifying":            DataFrame(~360 rows  — sprint weekends return 0),
            "pit_stops":             DataFrame(~1 200 rows),
            "driver_standings":      DataFrame(~480 rows),
            "constructor_standings": DataFrame(~240 rows),
        }
    """
    client = JolpicaClient()
    data = {
        "race_schedule":         [],
        "race_results":          [],
        "qualifying":            [],
        "pit_stops":             [],
        "driver_standings":      [],
        "constructor_standings": [],
    }

    logger.info("[Jolpica] Fetching race schedule for %d", season)
    rows = client.fetch_races(season)
    data["race_schedule"].extend(rows)
    logger.info("  %d races", len(rows))

    for rnd in rounds:
        logger.info("[Jolpica] Season %d Round %d — results", season, rnd)
        rows = client.fetch_results(season, rnd)
        data["race_results"].extend(rows)
        logger.info("  %d result rows", len(rows))

        logger.info("[Jolpica] Season %d Round %d — qualifying", season, rnd)
        rows = client.fetch_qualifying(season, rnd)
        data["qualifying"].extend(rows)
        logger.info("  %d qualifying rows (0 = sprint weekend)", len(rows))

        logger.info("[Jolpica] Season %d Round %d — pit stops", season, rnd)
        rows = client.fetch_pit_stops(season, rnd)
        data["pit_stops"].extend(rows)
        logger.info("  %d pit stop rows", len(rows))

        logger.info("[Jolpica] Season %d Round %d — driver standings", season, rnd)
        rows = client.fetch_driver_standings(season, rnd)
        data["driver_standings"].extend(rows)
        logger.info("  %d standing rows", len(rows))

        logger.info("[Jolpica] Season %d Round %d — constructor standings", season, rnd)
        rows = client.fetch_constructor_standings(season, rnd)
        data["constructor_standings"].extend(rows)
        logger.info("  %d constructor standing rows", len(rows))

    return {k: pd.DataFrame(v) for k, v in data.items()}


# ── OpenF1 fetch ───────────────────────────────────────────────────────────────

def fetch_openf1(season, round_nums, progress, race_schedule):
    """
    Fetch per-lap timing and tyre stint data from the OpenF1 API.

    OpenF1 organises data by session_key (not by season/round), so this
    function first fetches all Race sessions for the year, then joins them
    to the Jolpica race_schedule DataFrame on race_date (YYYY-MM-DD) to
    resolve each session_key → round number.

    Laps are fetched per-driver-per-session (one API call each) and skipped
    if already recorded in progress. For 2024 this is 24 sessions × ~20
    drivers = ~480 calls total.

    Args:
        season       — e.g. 2024
        round_nums   — list of round numbers to include, e.g. [1, 2, 3]
        progress     — in-memory progress dict from load_progress()
        race_schedule — DataFrame returned by fetch_jolpica()["race_schedule"];
                        must contain "race_date" (YYYY-MM-DD) and "round" columns

    Returns:
        {
            "laps":   DataFrame(~26 000 rows for 2024 — ~57 laps × 20 drivers × 24 rounds),
            "stints": DataFrame(~1 500 rows  — ~3 stints × 20 drivers × 24 rounds),
        }

    Both DataFrames are empty if no sessions could be matched to the requested rounds.
    """
    client = OpenF1Client()
    laps_rows = []
    stints_rows = []

    sessions_key = f"sessions_{season}"
    if not is_done(progress, "openf1", sessions_key):
        logger.info("[OpenF1] Fetching Race sessions for %d", season)
        sessions = client.fetch_race_sessions(season)
        mark_done(progress, "openf1", sessions_key, len(sessions))
    else:
        logger.info("[OpenF1] Sessions for %d already fetched — re-fetching for join", season)
        sessions = client.fetch_race_sessions(season)

    # Build session_key → round mapping using race_date join
    sessions_df = pd.DataFrame(sessions) if sessions else pd.DataFrame()
    if sessions_df.empty:
        logger.warning("No OpenF1 sessions found for %d", season)
        return {"laps": pd.DataFrame(), "stints": pd.DataFrame()}

    # Normalise date strings to YYYY-MM-DD for matching
    sessions_df["session_date"] = sessions_df["date_start"].str[:10]

    session_round_map = {}
    if not race_schedule.empty and "race_date" in race_schedule.columns:
        for _, sess in sessions_df.iterrows():
            matched = race_schedule[race_schedule["race_date"] == sess["session_date"]]
            if not matched.empty:
                rnd = int(matched.iloc[0]["round"])
                if rnd in round_nums:
                    session_round_map[sess["session_key"]] = rnd
                    logger.info("  Session %s matched → Round %d", sess["session_key"], rnd)
            else:
                logger.warning("  Session %s (date=%s) not matched to any race", sess["session_key"], sess["session_date"])

    if not session_round_map:
        logger.warning("No OpenF1 sessions matched to requested rounds. Laps will be empty.")
        return {"laps": pd.DataFrame(), "stints": pd.DataFrame()}

    for session_key, rnd in session_round_map.items():
        # Stints (per session)
        stint_key = f"stints_{session_key}"
        if not is_done(progress, "openf1", stint_key):
            logger.info("[OpenF1] Session %s (Round %d) — stints", session_key, rnd)
            rows = client.fetch_stints(session_key)
            stints_rows.extend(rows)
            mark_done(progress, "openf1", stint_key, len(rows))
            logger.info("  %d stint rows", len(rows))
        else:
            logger.info("[OpenF1] Session %s stints already fetched (skipping)", session_key)

        # Laps per driver
        drivers = client.fetch_drivers_for_session(session_key)
        driver_numbers = [d["driver_number"] for d in drivers]
        logger.info("[OpenF1] Session %s — %d drivers, fetching laps", session_key, len(driver_numbers))

        for drv in driver_numbers:
            lap_key = f"laps_{session_key}_d{drv}"
            if is_done(progress, "openf1", lap_key):
                continue
            rows = client.fetch_laps(session_key, drv)
            laps_rows.extend(rows)
            mark_done(progress, "openf1", lap_key, len(rows))

        logger.info("  %d lap rows so far (all drivers)", len(laps_rows))

    return {
        "laps":   pd.DataFrame(laps_rows)   if laps_rows   else pd.DataFrame(),
        "stints": pd.DataFrame(stints_rows) if stints_rows else pd.DataFrame(),
    }


# ── Save Parquet + upload ──────────────────────────────────────────────────────

def save_and_upload(df, table_name):
    """
    Write a DataFrame to a local Parquet file and upload it to the workspace.

    The local file lands in data/<table_name>.parquet (relative to project root).
    The workspace destination is WORKSPACE_DATA/<table_name>.parquet, which the
    Bronze notebook reads via spark.read.parquet().

    Skips silently (with a warning) if df is empty — this happens on sprint
    weekends where qualifying data is unavailable (Jolpica returns 0 rows).

    Example:
        save_and_upload(df_results, "bronze_race_results")
        # Writes  → data/bronze_race_results.parquet
        # Uploads → /Workspace/.../f1_data/bronze_race_results.parquet
    """
    if df.empty:
        logger.warning("Skipping %s — DataFrame is empty", table_name)
        return
    local_path = os.path.join(LOCAL_DATA_DIR, f"{table_name}.parquet")
    df.to_parquet(local_path, index=False)
    logger.info("Saved %s: %d rows → %s", table_name, len(df), local_path)
    upload_to_workspace(local_path, f"{WORKSPACE_DATA}/{table_name}.parquet")


# ── Main ───────────────────────────────────────────────────────────────────────

def main():
    """
    Entry point: parse CLI arguments, fetch all F1 data, and upload to Databricks.

    Workflow:
        1. Parse --season, --round, --mode, --reset-progress from the CLI.
        2. Load (or reset) the resume progress cache.
        3. Determine which rounds to fetch — one specific round or all rounds
           in the season (via get_round_count).
        4. Fetch Jolpica tables (schedule, results, qualifying, pit stops,
           driver/constructor standings) for all requested rounds.
        5. Fetch OpenF1 tables (laps, stints) matched to the same rounds via
           the session_key → race_date join.
        6. Save each DataFrame as a local Parquet file and upload to the
           Databricks workspace so the Bronze notebook can read it.

    The function is resumable: already-completed fetch tasks are skipped based
    on the progress cache, so interrupted runs can be safely restarted.
    """
    parser = argparse.ArgumentParser(description="Fetch F1 data and upload to Databricks workspace")
    parser.add_argument("--season",         type=int, required=True,      help="F1 season year (e.g. 2024)")
    parser.add_argument("--round",          type=int, default=None,       help="Specific round number (omit for all rounds)")
    parser.add_argument("--mode",           choices=["full", "incremental"], default="full")
    parser.add_argument("--reset-progress", action="store_true",          help="Clear resume cache and re-fetch everything")
    args = parser.parse_args()

    os.makedirs(LOCAL_DATA_DIR, exist_ok=True)

    if args.reset_progress and os.path.exists(PROGRESS_FILE):
        os.remove(PROGRESS_FILE)
        logger.info("Progress cache cleared")

    progress = load_progress()

    # Determine which rounds to fetch
    client = JolpicaClient()
    if args.round is not None:
        rounds = [args.round]
    else:
        total_rounds = client.get_round_count(args.season)
        rounds = list(range(1, total_rounds + 1))
        logger.info("Season %d has %d rounds", args.season, total_rounds)

    logger.info("Fetching Jolpica data for season=%d rounds=%s", args.season, rounds)
    jolpica_data = fetch_jolpica(args.season, rounds)

    logger.info("Fetching OpenF1 data for season=%d rounds=%s", args.season, rounds)
    openf1_data = fetch_openf1(args.season, rounds, progress, jolpica_data["race_schedule"])

    # Ensure workspace directory exists
    ensure_workspace_dir()

    # Save + upload all tables
    save_and_upload(jolpica_data["race_schedule"],         "bronze_race_schedule")
    save_and_upload(jolpica_data["race_results"],          "bronze_race_results")
    save_and_upload(jolpica_data["qualifying"],            "bronze_qualifying")
    save_and_upload(jolpica_data["pit_stops"],             "bronze_pit_stops")
    save_and_upload(jolpica_data["driver_standings"],      "bronze_driver_standings")
    save_and_upload(jolpica_data["constructor_standings"], "bronze_constructor_standings")
    save_and_upload(openf1_data["laps"],                   "bronze_laps")
    save_and_upload(openf1_data["stints"],                 "bronze_stints")

    logger.info("\nAll done. Files uploaded to %s", WORKSPACE_DATA)
    logger.info("Run the Bronze notebook — it will read from the workspace Parquet files.")


if __name__ == "__main__":
    main()
