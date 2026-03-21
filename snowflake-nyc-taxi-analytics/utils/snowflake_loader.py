"""
Snowflake connection helper and bulk-load utilities.

All credentials are read from environment variables so that secrets never
appear in source code or configuration files.  Set these before running:

  export SNOWFLAKE_ACCOUNT=<your-account-identifier>   # e.g. xy12345.us-east-1
  export SNOWFLAKE_USER=<username>
  export SNOWFLAKE_PASSWORD=<password>
  export SNOWFLAKE_ROLE=SYSADMIN                        # optional, defaults to SYSADMIN
"""

import os

import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def get_connection(database="NYC_TAXI", schema="RAW"):
    """
    Open and return a Snowflake connection.

    Parameters
    ----------
    database : str
        Target Snowflake database.  Defaults to 'NYC_TAXI' (dev).
    schema : str
        Target schema within that database.  Defaults to 'RAW'.

    Returns
    -------
    snowflake.connector.SnowflakeConnection
        An open connection.  Callers are responsible for closing it (or using
        it as a context manager).
    """
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        role=os.environ.get("SNOWFLAKE_ROLE", "SYSADMIN"),
        warehouse="COMPUTE_WH",
        database=database,
        schema=schema,
    )


def ensure_raw_table(conn):
    """
    Create the RAW.YELLOW_TRIPDATA table if it does not already exist.

    This table is the landing zone for all TLC Parquet loads.  dbt reads from
    it via the 'yellow_tripdata' source defined in models/sources.yml.

    Column names match the renamed/snake_cased fields produced by
    scripts/load_to_snowflake.py.
    """
    ddl = """
    CREATE TABLE IF NOT EXISTS RAW.YELLOW_TRIPDATA (
        vendor_id             INTEGER,
        pickup_datetime       TIMESTAMP_NTZ,
        dropoff_datetime      TIMESTAMP_NTZ,
        passenger_count       FLOAT,
        trip_distance         FLOAT,
        rate_code_id          FLOAT,
        store_and_fwd_flag    VARCHAR(1),
        pickup_location_id    INTEGER,
        dropoff_location_id   INTEGER,
        payment_type          INTEGER,
        fare_amount           FLOAT,
        extra                 FLOAT,
        mta_tax               FLOAT,
        tip_amount            FLOAT,
        tolls_amount          FLOAT,
        improvement_surcharge FLOAT,
        total_amount          FLOAT,
        congestion_surcharge  FLOAT,
        airport_fee           FLOAT,
        _loaded_at            TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
        _source_month         VARCHAR(7)
    )
    """
    with conn.cursor() as cur:
        cur.execute(ddl)


def load_dataframe(conn, df, table_name="YELLOW_TRIPDATA", schema="RAW"):
    """
    Append a pandas DataFrame to a Snowflake table using write_pandas.

    write_pandas uses the Snowflake PUT + COPY pattern internally, which is
    significantly faster than row-by-row INSERT for large datasets.

    Parameters
    ----------
    conn : snowflake.connector.SnowflakeConnection
    df   : pandas.DataFrame
        Must have column names that already match the target table (upper-cased
        by write_pandas automatically).
    table_name : str
        Target table name (default 'YELLOW_TRIPDATA').
    schema : str
        Target schema (default 'RAW').

    Returns
    -------
    tuple
        (success, num_chunks, num_rows, output) as returned by write_pandas.
    """
    success, num_chunks, num_rows, output = write_pandas(
        conn=conn,
        df=df,
        table_name=table_name,
        schema=schema,
        auto_create_table=False,
        overwrite=False,
        quote_identifiers=False,
    )
    return success, num_chunks, num_rows, output
