"""Streamlit dashboard for Seattle Police Use of Force data."""

from __future__ import annotations

import sqlite3

import pandas as pd
import plotly.express as px
import streamlit as st

from pipeline.update import run_batch_update


st.set_page_config(page_title="Seattle UOF Dashboard", layout="wide")
DB_PATH = "db/seattle_uof.db"


@st.cache_resource
def get_db_connection() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    return conn


@st.cache_data(ttl=300)
def run_query(query: str) -> pd.DataFrame:
    conn = get_db_connection()
    return pd.read_sql_query(query, conn)


@st.cache_data(ttl=300)
def get_joined_data() -> pd.DataFrame:
    query = """
    SELECT
        i.uniqueid,
        i.incident_num,
        it.type_name,
        it.force_level,
        i.incident_date,
        i.incident_time,
        i.incident_year,
        i.day_of_week,
        i.hour_of_day,
        p.precinct_name,
        s.sector_name,
        i.beat,
        i.officer_id,
        i.subject_id,
        r.race_name,
        g.gender_name
    FROM incidents i
    JOIN incident_types it ON i.type_id = it.type_id
    LEFT JOIN precincts p ON i.precinct_id = p.precinct_id
    LEFT JOIN sectors s ON i.sector_id = s.sector_id
    LEFT JOIN races r ON i.race_id = r.race_id
    LEFT JOIN genders g ON i.gender_id = g.gender_id
    """
    return run_query(query)


@st.cache_data(ttl=60)
def get_last_update_log() -> pd.DataFrame:
    return run_query(
        """
        SELECT run_timestamp, records_fetched, records_new, records_updated, errors, status
        FROM update_log
        ORDER BY log_id DESC
        """
    )


def apply_filters(df: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.header("Filters")

    years = sorted([int(y) for y in df["incident_year"].dropna().unique()])
    if years:
        year_range = st.sidebar.slider(
            "Year range",
            min_value=min(years),
            max_value=max(years),
            value=(min(years), max(years)),
        )
    else:
        year_range = (0, 9999)

    levels = sorted([int(x) for x in df["force_level"].dropna().unique()])
    selected_levels = st.sidebar.multiselect("Force level", options=levels, default=levels)

    precincts = sorted(df["precinct_name"].dropna().unique().tolist())
    selected_precincts = st.sidebar.multiselect(
        "Precinct", options=precincts, default=precincts
    )

    races = sorted(df["race_name"].dropna().unique().tolist())
    selected_races = st.sidebar.multiselect("Subject race", options=races, default=races)

    genders = sorted(df["gender_name"].dropna().unique().tolist())
    selected_genders = st.sidebar.multiselect(
        "Subject gender", options=genders, default=genders
    )

    filtered = df.copy()
    filtered = filtered[
        filtered["incident_year"].fillna(-1).between(year_range[0], year_range[1])
    ]

    if selected_levels:
        filtered = filtered[filtered["force_level"].isin(selected_levels)]
    if selected_precincts:
        filtered = filtered[filtered["precinct_name"].isin(selected_precincts)]
    if selected_races:
        filtered = filtered[filtered["race_name"].isin(selected_races)]
    if selected_genders:
        filtered = filtered[filtered["gender_name"].isin(selected_genders)]

    return filtered


def render_overview_tab(df: pd.DataFrame) -> None:
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Distinct Incidents", int(df["incident_num"].nunique()))
    c2.metric("Level 1", int((df["force_level"] == 1).sum()))
    c3.metric("Level 2", int((df["force_level"] == 2).sum()))
    c4.metric("Level 3", int((df["force_level"] == 3).sum()))

    by_year = df.groupby("incident_year", dropna=True).size().reset_index(name="count")
    by_level = df.groupby("force_level", dropna=True).size().reset_index(name="count")
    by_precinct = (
        df.groupby("precinct_name", dropna=True).size().reset_index(name="count").sort_values("count", ascending=False)
    )
    by_hour = df.groupby("hour_of_day", dropna=True).size().reset_index(name="count")
    by_day = (
        df.groupby("day_of_week", dropna=True)
        .size()
        .reset_index(name="count")
        .sort_values("count", ascending=False)
    )

    st.plotly_chart(px.bar(by_year, x="incident_year", y="count", title="Incidents by Year"), use_container_width=True)
    st.plotly_chart(px.pie(by_level, names="force_level", values="count", hole=0.45, title="By Force Level"), use_container_width=True)
    st.plotly_chart(px.bar(by_precinct, x="precinct_name", y="count", title="By Precinct"), use_container_width=True)
    st.plotly_chart(px.line(by_hour, x="hour_of_day", y="count", title="By Hour of Day"), use_container_width=True)
    st.plotly_chart(px.bar(by_day, x="day_of_week", y="count", title="By Day of Week"), use_container_width=True)


def render_demographics_tab(df: pd.DataFrame) -> None:
    by_race = df.groupby("race_name", dropna=True).size().reset_index(name="count").sort_values("count", ascending=False)
    race_x_level = df.groupby(["race_name", "force_level"], dropna=True).size().reset_index(name="count")
    by_gender = df.groupby("gender_name", dropna=True).size().reset_index(name="count")
    gender_x_level = df.groupby(["gender_name", "force_level"], dropna=True).size().reset_index(name="count")

    st.plotly_chart(px.bar(by_race, x="race_name", y="count", title="By Subject Race"), use_container_width=True)
    st.plotly_chart(
        px.bar(race_x_level, x="race_name", y="count", color="force_level", title="Race x Force Level", barmode="stack"),
        use_container_width=True,
    )
    st.plotly_chart(px.bar(by_gender, x="gender_name", y="count", title="By Subject Gender"), use_container_width=True)
    st.plotly_chart(
        px.bar(gender_x_level, x="gender_name", y="count", color="force_level", title="Gender x Force Level", barmode="stack"),
        use_container_width=True,
    )


def render_explorer_tab(df: pd.DataFrame) -> None:
    st.dataframe(df, use_container_width=True)
    st.download_button(
        "Download filtered data as CSV",
        data=df.to_csv(index=False).encode("utf-8"),
        file_name="uof_filtered.csv",
        mime="text/csv",
    )


def render_sql_tab() -> None:
    st.caption("Read-only SQL mode")
    default_query = """
SELECT i.incident_date, it.type_name, p.precinct_name, r.race_name, g.gender_name
FROM incidents i
JOIN incident_types it ON i.type_id = it.type_id
LEFT JOIN precincts p ON i.precinct_id = p.precinct_id
LEFT JOIN races r ON i.race_id = r.race_id
LEFT JOIN genders g ON i.gender_id = g.gender_id
ORDER BY i.incident_date DESC
LIMIT 100;
    """.strip()

    sql = st.text_area("SQL", value=default_query, height=180)
    if st.button("Execute SQL"):
        if not sql.strip().lower().startswith(("select", "with")):
            st.error("Only SELECT/CTE queries are allowed.")
            return
        try:
            with sqlite3.connect(f"file:{DB_PATH}?mode=ro", uri=True) as conn:
                result = pd.read_sql_query(sql, conn)
            st.dataframe(result, use_container_width=True)
        except Exception as exc:  # noqa: BLE001
            st.error(f"Query error: {exc}")


def render_update_log_tab() -> None:
    logs = get_last_update_log()
    if logs.empty:
        st.info("No update runs logged yet.")
        return
    st.write("Last refresh:", logs.iloc[0]["run_timestamp"])
    st.dataframe(logs, use_container_width=True)


def main() -> None:
    st.title("Seattle Police Use of Force Dashboard")

    left, right = st.columns([1, 3])
    with left:
        if st.button("Refresh Data"):
            with st.spinner("Running full batch update..."):
                result = run_batch_update(DB_PATH, "https://data.seattle.gov/resource/ppi5-g2bj.json")
            run_query.clear()
            get_joined_data.clear()
            get_last_update_log.clear()
            if result["status"] == "success":
                st.success(
                    f"Update successful: fetched={result['total_fetched']}, new={result['new_records']}, updated={result['updated_records']}"
                )
            else:
                st.error(f"Update failed: {result['errors']}")

    df = get_joined_data()
    filtered = apply_filters(df)

    tabs = st.tabs(["Overview", "Demographics", "Data Explorer", "SQL Query", "Update Log"])
    with tabs[0]:
        render_overview_tab(filtered)
    with tabs[1]:
        render_demographics_tab(filtered)
    with tabs[2]:
        render_explorer_tab(filtered)
    with tabs[3]:
        render_sql_tab()
    with tabs[4]:
        render_update_log_tab()


if __name__ == "__main__":
    main()
