"""
Load one month of TLC Yellow Taxi Parquet data into Snowflake RAW.YELLOW_TRIPDATA.

This script performs the E and L in ELT — dbt handles the T.  Each invocation:
  1. Reads the local Parquet file into pandas.
  2. Renames TLC column names to clean snake_case.
  3. Adds two metadata columns: _loaded_at and _source_month.
  4. Ensures the target Snowflake table exists (idempotent DDL).
  5. Appends the data via write_pandas (PUT + COPY internally).

Loading is additive (append-only).  Running the same month twice will produce
duplicate rows — guard against this by checking before loading or truncating.
The dbt incremental model (fct_trips) deduplicates on trip_id.

Usage:
    make load MONTH=2024-01
    # or directly:
    python scripts/load_to_snowflake.py --month 2024-01

Required environment variables:
    SNOWFLAKE_ACCOUNT, SNOWFLAKE_USER, SNOWFLAKE_PASSWORD
Optional:
    SNOWFLAKE_ROLE  (default: SYSADMIN)
"""

import argparse
from datetime import datetime, timezone
from pathlib import Path

import pandas as pd

from utils.snowflake_loader import ensure_raw_table, get_connection, load_dataframe

DATA_DIR = Path(__file__).parent.parent / "data" / "raw"

# Mapping from TLC column names → clean snake_case names used in Snowflake + dbt models.
# Keeping this explicit makes schema evolution easy to track in git history.
COLUMN_RENAMES = {
    "VendorID": "vendor_id",
    "tpep_pickup_datetime": "pickup_datetime",
    "tpep_dropoff_datetime": "dropoff_datetime",
    "passenger_count": "passenger_count",
    "trip_distance": "trip_distance",
    "RatecodeID": "rate_code_id",
    "store_and_fwd_flag": "store_and_fwd_flag",
    "PULocationID": "pickup_location_id",
    "DOLocationID": "dropoff_location_id",
    "payment_type": "payment_type",
    "fare_amount": "fare_amount",
    "extra": "extra",
    "mta_tax": "mta_tax",
    "tip_amount": "tip_amount",
    "tolls_amount": "tolls_amount",
    "improvement_surcharge": "improvement_surcharge",
    "total_amount": "total_amount",
    "congestion_surcharge": "congestion_surcharge",
    "Airport_fee": "airport_fee",
}


def prepare_dataframe(path, month):
    """
    Read Parquet, rename columns, add metadata fields.

    Parameters
    ----------
    path  : Path   – local Parquet file path
    month : str    – YYYY-MM string used as _source_month value

    Returns
    -------
    pandas.DataFrame ready for write_pandas upload.
    """
    print(f"[read]     {path.name}")
    df = pd.read_parquet(path)
    print(f"           {len(df):,} rows  |  columns: {list(df.columns)}")

    # Rename only the columns we know about; drop any unexpected extras.
    df = df.rename(columns=COLUMN_RENAMES)
    known_cols = list(COLUMN_RENAMES.values())
    df = df[[c for c in known_cols if c in df.columns]]

    # Metadata columns — helps with auditing and incremental debugging.
    df["_loaded_at"] = datetime.now(timezone.utc).replace(tzinfo=None)
    df["_source_month"] = month

    # write_pandas requires uppercase column names to match Snowflake identifiers.
    df.columns = [c.upper() for c in df.columns]

    print(f"[prepared] {len(df):,} rows  |  {len(df.columns)} columns")
    return df


def main():
    parser = argparse.ArgumentParser(
        description="Load one month of TLC Yellow Taxi data into Snowflake"
    )
    parser.add_argument(
        "--month",
        required=True,
        metavar="YYYY-MM",
        help="Month to load, e.g. 2024-01",
    )
    args = parser.parse_args()

    parquet_path = DATA_DIR / f"yellow_tripdata_{args.month}.parquet"
    if not parquet_path.exists():
        raise FileNotFoundError(
            f"Parquet file not found: {parquet_path}\n"
            f"Run:  make download MONTHS={args.month}"
        )

    df = prepare_dataframe(parquet_path, args.month)

    print("[connect]  Opening Snowflake connection ...")
    with get_connection(database="NYC_TAXI", schema="RAW") as conn:
        ensure_raw_table(conn)
        print("[load]     Writing to RAW.YELLOW_TRIPDATA via write_pandas ...")
        success, num_chunks, num_rows, _ = load_dataframe(conn, df)

    if success:
        print(f"[done]     Loaded {num_rows:,} rows in {num_chunks} chunk(s).")
        print(f"           Month {args.month} is now available in RAW.YELLOW_TRIPDATA.")
    else:
        raise RuntimeError("write_pandas reported failure — check Snowflake logs.")


if __name__ == "__main__":
    main()
