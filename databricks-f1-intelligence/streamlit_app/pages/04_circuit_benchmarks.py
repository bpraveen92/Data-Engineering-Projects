"""Circuit benchmarks — fastest laps, pole times, race winners per circuit."""

import streamlit as st
import plotly.express as px
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from app import run_query, table

st.title("📍 Circuit Benchmarks")

rows = run_query(f"""
    SELECT
        circuit_name                                            AS Circuit,
        country                                                 AS Country,
        ROUND(all_time_fastest_lap_seconds, 3)                  AS Fastest_Lap_s,
        fastest_lap_driver_id                                   AS FL_Driver,
        fastest_lap_season                                      AS FL_Season,
        ROUND(all_time_pole_seconds, 3)                         AS Pole_s,
        pole_driver_id                                          AS Pole_Driver,
        pole_season                                             AS Pole_Season,
        total_races_held                                        AS Races_Held,
        last_race_winner_id                                     AS Last_Winner,
        last_race_season                                        AS Last_Season
    FROM {table('gold_circuit_benchmarks')}
    ORDER BY circuit_name
""")

if rows:
    df = pd.DataFrame(rows)

    # Search filter
    search = st.text_input("Search circuit", "")
    if search:
        df = df[df["Circuit"].str.contains(search, case=False, na=False)]

    st.dataframe(df, use_container_width=True, hide_index=True)

    # Fastest lap bar chart
    df_valid = df[df["Fastest_Lap_s"].notna()].sort_values("Fastest_Lap_s")
    if not df_valid.empty:
        fig = px.bar(
            df_valid,
            x="Circuit", y="Fastest_Lap_s",
            color="FL_Driver",
            title="All-Time Fastest Race Lap by Circuit (seconds)",
            labels={"Fastest_Lap_s": "Lap Time (s)", "FL_Driver": "Driver"},
        )
        fig.update_layout(xaxis_tickangle=-45, height=450)
        st.plotly_chart(fig, use_container_width=True)

    # Pole time bar chart
    df_pole = df[df["Pole_s"].notna()].sort_values("Pole_s")
    if not df_pole.empty:
        fig_pole = px.bar(
            df_pole,
            x="Circuit", y="Pole_s",
            color="Pole_Driver",
            title="All-Time Pole Position Time by Circuit (seconds)",
            labels={"Pole_s": "Qualifying Time (s)", "Pole_Driver": "Driver"},
        )
        fig_pole.update_layout(xaxis_tickangle=-45, height=450)
        st.plotly_chart(fig_pole, use_container_width=True)
else:
    st.info("No circuit benchmark data available yet.")
