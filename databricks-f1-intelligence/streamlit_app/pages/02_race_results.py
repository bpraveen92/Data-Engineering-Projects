"""Race results — filterable by season and round with podium highlighting."""

import streamlit as st
import pandas as pd
import sys, os
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))
from app import run_query, table

st.title("🏁 Race Results")

seasons = run_query(f"SELECT DISTINCT season FROM {table('silver_race_results')} ORDER BY season DESC")
season_list = [r["season"] for r in seasons]
selected_season = st.sidebar.selectbox("Season", season_list, index=0)

rounds = run_query(f"""
    SELECT DISTINCT round, race_name
    FROM {table('silver_race_results')}
    WHERE season = {selected_season}
    ORDER BY round
""")
round_options = {r["race_name"]: r["round"] for r in rounds}
selected_race  = st.sidebar.selectbox("Race", list(round_options.keys()))
selected_round = round_options[selected_race]

# ── Race result table ──────────────────────────────────────────────────────────
rows = run_query(f"""
    SELECT
        final_position   AS Pos,
        driver_code      AS Driver,
        constructor_id   AS Team,
        grid_position    AS Grid,
        laps_completed   AS Laps,
        status_category  AS Status,
        points           AS Pts,
        ROUND(fastest_lap_seconds, 3)     AS Best_Lap_s,
        ROUND(gap_to_winner_seconds, 3)   AS Gap_s
    FROM {table('silver_race_results')}
    WHERE season = {selected_season}
      AND round  = {selected_round}
    ORDER BY COALESCE(final_position, 99)
""")

if rows:
    df = pd.DataFrame(rows)

    def highlight_podium(row):
        pos = row.get("Pos")
        if pos == 1:
            return ["background-color: #FFD700"] * len(row)  # gold
        elif pos == 2:
            return ["background-color: #C0C0C0"] * len(row)  # silver
        elif pos == 3:
            return ["background-color: #CD7F32"] * len(row)  # bronze
        return [""] * len(row)

    st.dataframe(
        df.style.apply(highlight_podium, axis=1),
        use_container_width=True,
        hide_index=True,
    )

    # Fastest lap highlight
    fl = run_query(f"""
        SELECT driver_code, fastest_lap_seconds, fastest_lap_speed_kph
        FROM {table('silver_race_results')}
        WHERE season = {selected_season}
          AND round  = {selected_round}
          AND fastest_lap_rank = 1
        LIMIT 1
    """)
    if fl:
        f = fl[0]
        st.success(
            f"**Fastest Lap**: {f['driver_code']} — "
            f"{f['fastest_lap_seconds']:.3f}s "
            f"({f['fastest_lap_speed_kph']:.1f} km/h)"
        )
else:
    st.info("No race results available for the selected round.")

# ── Qualifying results ──────────────────────────────────────────────────────────
st.subheader("Qualifying")
qual = run_query(f"""
    SELECT
        qualifying_position AS Pos,
        driver_code         AS Driver,
        constructor_id      AS Team,
        ROUND(q1_seconds, 3)                    AS Q1_s,
        ROUND(q2_seconds, 3)                    AS Q2_s,
        ROUND(q3_seconds, 3)                    AS Q3_s,
        ROUND(gap_to_pole_seconds, 3)           AS Gap_s
    FROM {table('silver_qualifying')}
    WHERE season = {selected_season}
      AND round  = {selected_round}
    ORDER BY qualifying_position
""")
if qual:
    st.dataframe(pd.DataFrame(qual), use_container_width=True, hide_index=True)
else:
    st.info("No qualifying data (sprint weekend or data not yet loaded).")
