"""Tyre strategy — compound heatmap and stint breakdown per race."""

import streamlit as st
import plotly.graph_objects as go
import plotly.express as px
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from app import run_query, table

st.title("🛞 Tyre Strategy")

COMPOUND_COLOURS = {
    "SOFT":         "#FF3333",
    "MEDIUM":       "#FFD700",
    "HARD":         "#BBBBBB",
    "INTERMEDIATE": "#39B54A",
    "WET":          "#0067FF",
    "UNKNOWN":      "#888888",
}

seasons = run_query(f"SELECT DISTINCT season FROM {table('gold_tyre_strategy_report')} ORDER BY season DESC")
season_list = [r["season"] for r in seasons]
selected_season = st.sidebar.selectbox("Season", season_list, index=0)

rounds = run_query(f"""
    SELECT DISTINCT round, circuit_id
    FROM {table('gold_tyre_strategy_report')}
    WHERE season = {selected_season}
    ORDER BY round
""")
round_options = {r["circuit_id"]: r["round"] for r in rounds}
selected_circuit = st.sidebar.selectbox("Race", list(round_options.keys()))
selected_round   = round_options[selected_circuit]

rows = run_query(f"""
    SELECT
        final_position      AS Pos,
        driver_code         AS Driver,
        constructor_id      AS Team,
        total_stints        AS Stints,
        compounds_used      AS Compounds,
        stint_1_compound    AS S1,
        stint_1_laps        AS S1_laps,
        stint_2_compound    AS S2,
        stint_2_laps        AS S2_laps,
        stint_3_compound    AS S3,
        stint_3_laps        AS S3_laps,
        pit_count           AS Pits
    FROM {table('gold_tyre_strategy_report')}
    WHERE season = {selected_season}
      AND round  = {selected_round}
    ORDER BY COALESCE(final_position, 99)
""")

if rows:
    df = pd.DataFrame(rows)
    st.dataframe(df, use_container_width=True, hide_index=True)

    # Compound distribution pie chart
    compound_counts = {}
    for row in rows:
        for c in (row.get("Compounds") or "").split(","):
            c = c.strip()
            if c:
                compound_counts[c] = compound_counts.get(c, 0) + 1

    if compound_counts:
        fig_pie = go.Figure(go.Pie(
            labels=list(compound_counts.keys()),
            values=list(compound_counts.values()),
            marker_colors=[COMPOUND_COLOURS.get(c, "#888") for c in compound_counts],
            hole=0.3,
        ))
        fig_pie.update_layout(title_text=f"Compound Usage — {selected_circuit} {selected_season}")
        st.plotly_chart(fig_pie, use_container_width=True)
else:
    st.info("No tyre strategy data for the selected race.")

st.subheader("Lap-by-Lap Tyre Timeline")
laps = run_query(f"""
    SELECT driver_code, lap_number, compound, lap_duration_seconds
    FROM {table('silver_lap_analysis')}
    WHERE season = {selected_season}
      AND round  = {selected_round}
      AND compound IS NOT NULL
    ORDER BY driver_code, lap_number
""")

if laps:
    df_laps = pd.DataFrame(laps)
    top_drivers = df_laps["driver_code"].value_counts().head(10).index.tolist()
    df_top = df_laps[df_laps["driver_code"].isin(top_drivers)]

    fig = px.scatter(
        df_top,
        x="lap_number", y="driver_code",
        color="compound",
        color_discrete_map=COMPOUND_COLOURS,
        title="Tyre Compound per Lap (Top 10 drivers)",
        labels={"lap_number": "Lap", "driver_code": "Driver", "compound": "Compound"},
        size_max=8,
    )
    fig.update_traces(marker=dict(size=10, symbol="square"))
    fig.update_layout(height=400)
    st.plotly_chart(fig, use_container_width=True)
else:
    st.info("Lap telemetry data not available for the selected race.")
