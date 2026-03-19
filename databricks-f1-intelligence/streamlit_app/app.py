"""
F1 Intelligence — Streamlit entry point.

Shared database connection and query helpers used by all pages.
"""

import os
import streamlit as st
from databricks import sql as dbsql
from databricks.sdk import WorkspaceClient

st.set_page_config(
    page_title="F1 Intelligence",
    page_icon="🏎",
    layout="wide",
    initial_sidebar_state="expanded",
)

CATALOG   = os.getenv("CATALOG",  "f1_intelligence")
SCHEMA    = os.getenv("SCHEMA",   "f1_dev")
# Databricks Apps auto-injects DATABRICKS_HOST and DATABRICKS_TOKEN for the
# app service principal. DATABRICKS_HTTP_PATH is set explicitly in app.yaml.
HOSTNAME  = os.getenv("DATABRICKS_HOST", os.getenv("DATABRICKS_SERVER_HOSTNAME", ""))
HTTP_PATH = os.getenv("DATABRICKS_HTTP_PATH", "")
TOKEN     = os.getenv("DATABRICKS_TOKEN", "")


@st.cache_resource
def get_connection():
    # Inside Databricks Apps, OAuth M2M credentials are injected automatically.
    # Using WorkspaceClient as the credential provider handles both OAuth (Apps)
    # and PAT (local dev) without needing to switch code paths.
    w = WorkspaceClient()
    return dbsql.connect(
        server_hostname=w.config.host.replace("https://", ""),
        http_path=HTTP_PATH,
        credentials_provider=w.config.authenticate,
    )


def run_query(sql: str) -> list[dict]:
    conn = get_connection()
    with conn.cursor() as cursor:
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        return [dict(zip(columns, row)) for row in cursor.fetchall()]


def table(name: str) -> str:
    return f"{CATALOG}.{SCHEMA}.{name}"


# ── Home page ──────────────────────────────────────────────────────────────────

st.title("🏎 F1 Intelligence")
st.markdown("Formula 1 analytics powered by **Databricks Medallion Architecture**.")

try:
    rows = run_query(f"""
        SELECT
            MAX(races_completed)               AS latest_round,
            COUNT(DISTINCT driver_id)          AS drivers,
            COUNT(DISTINCT constructor_id)     AS teams,
            MAX(season)                        AS season
        FROM {table('gold_driver_championship')}
    """)
    if rows:
        r = rows[0]
        col1, col2, col3, col4 = st.columns(4)
        col1.metric("Season", r["season"])
        col2.metric("Rounds Completed", r["latest_round"])
        col3.metric("Drivers", r["drivers"])
        col4.metric("Constructors", r["teams"])

    leader = run_query(f"""
        SELECT driver_code, constructor_id, current_points, wins
        FROM {table('gold_driver_championship')}
        WHERE season = (SELECT MAX(season) FROM {table('gold_driver_championship')})
        ORDER BY current_position
        LIMIT 1
    """)
    if leader:
        l = leader[0]
        st.info(
            f"**Championship Leader**: {l['driver_code']} ({l['constructor_id']}) "
            f"— {l['current_points']} pts, {l['wins']} wins"
        )
except Exception as e:
    st.warning(f"Could not load KPIs: {e}")

st.markdown("---")
st.markdown(
    "Navigate using the sidebar: **Championship**, **Race Results**, "
    "**Tyre Strategy**, **Circuit Benchmarks**."
)
