"""Championship standings — current table + cumulative points progression."""

import streamlit as st
import plotly.express as px
import plotly.graph_objects as go
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from app import run_query, table

st.title("🏆 Championship Standings")

seasons = run_query(f"SELECT DISTINCT season FROM {table('gold_driver_championship')} ORDER BY season DESC")
season_list = [r["season"] for r in seasons]
selected_season = st.sidebar.selectbox("Season", season_list, index=0)

st.subheader("Driver Championship")
rows = run_query(f"""
    SELECT
        current_position AS Pos,
        driver_code      AS Driver,
        constructor_id   AS Team,
        current_points   AS Points,
        wins             AS Wins,
        podiums          AS Podiums,
        dnfs             AS DNFs,
        races_completed  AS Races
    FROM {table('gold_driver_championship')}
    WHERE season = {selected_season}
    ORDER BY current_position
""")

if rows:
    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)
else:
    st.info("No driver championship data available yet.")

st.subheader("Points Progression by Round")
prog = run_query(f"""
    SELECT round, driver_code, points
    FROM {table('silver_driver_standings')}
    WHERE season = {selected_season}
    ORDER BY round, position
""")

if prog:
    df_prog = pd.DataFrame(prog)
    top_drivers = (
        df_prog.groupby("driver_code")["points"].max()
        .nlargest(10).index.tolist()
    )
    df_top = df_prog[df_prog["driver_code"].isin(top_drivers)]
    fig = px.line(
        df_top,
        x="round", y="points", color="driver_code",
        title=f"{selected_season} Driver Championship Progression (Top 10)",
        labels={"round": "Round", "points": "Cumulative Points", "driver_code": "Driver"},
        markers=True,
    )
    fig.update_layout(legend_title_text="Driver", height=450)
    st.plotly_chart(fig, use_container_width=True)

st.subheader("Constructor Championship")
cs = run_query(f"""
    SELECT
        current_position AS Pos,
        constructor_name AS Constructor,
        current_points   AS Points,
        wins             AS Wins,
        podiums          AS Podiums,
        races_completed  AS Races
    FROM {table('gold_constructor_championship')}
    WHERE season = {selected_season}
    ORDER BY current_position
""")

if cs:
    df_cs = pd.DataFrame(cs)
    fig_bar = px.bar(
        df_cs,
        x="Constructor", y="Points",
        title=f"{selected_season} Constructor Points",
        color="Constructor",
    )
    st.plotly_chart(fig_bar, use_container_width=True)
    st.dataframe(df_cs, use_container_width=True, hide_index=True)
else:
    st.info("No constructor championship data available yet.")
