from pathlib import Path

import pandas as pd
import pydeck as pdk
import streamlit as st
from deltalake import DeltaTable


ROOT_DIR = Path(__file__).resolve().parents[1]
WAREHOUSE_DIR = ROOT_DIR / "warehouse"
FACT_TRAFFIC_PATH = WAREHOUSE_DIR / "fact_traffic"
DIM_ZONE_PATH = WAREHOUSE_DIR / "dim_zone"
DIM_ROAD_PATH = WAREHOUSE_DIR / "dim_road"

ZONE_COORDS = {
    "CBD": {"lat": 51.5076, "lon": -0.1278},
    "TECHPARK": {"lat": 51.5158, "lon": -0.0987},
    "AIRPORT": {"lat": 51.4700, "lon": -0.4543},
    "TRAINSTATION": {"lat": 51.5319, "lon": -0.1233},
    "SUBURB": {"lat": 51.5484, "lon": -0.1982},
}


def inject_theme() -> None:
    st.markdown(
        """
        <style>
        .stApp {
            background:
                radial-gradient(circle at top right, rgba(69, 123, 91, 0.18), transparent 28%),
                linear-gradient(180deg, #f5faf6 0%, #edf4ee 100%);
        }
        [data-testid="stSidebar"] {
            background: linear-gradient(180deg, #17362a 0%, #214735 100%);
        }
        [data-testid="stSidebar"] * {
            color: #f3faf4;
        }
        .hero-card {
            padding: 1.4rem 1.5rem;
            border-radius: 22px;
            border: 1px solid rgba(32, 68, 51, 0.08);
            background: linear-gradient(135deg, #163528 0%, #24533e 100%);
            color: #f4fbf5;
            box-shadow: 0 20px 50px rgba(22, 53, 40, 0.12);
            margin-bottom: 1rem;
        }
        .hero-eyebrow {
            font-size: 0.82rem;
            letter-spacing: 0.08em;
            text-transform: uppercase;
            color: #b7d9c0;
            margin-bottom: 0.5rem;
        }
        .hero-title {
            font-size: 2.4rem;
            font-weight: 700;
            line-height: 1.05;
            margin-bottom: 0.5rem;
        }
        .hero-copy {
            color: #d4ead9;
            font-size: 1rem;
            max-width: 52rem;
        }
        .section-title {
            font-size: 1.1rem;
            font-weight: 700;
            color: #183628;
            margin: 0.3rem 0 0.8rem 0;
        }
        </style>
        """,
        unsafe_allow_html=True,
    )


def metric_card(label: str, value: str, delta: str) -> None:
    st.markdown(
        f"""
        <div style="padding:1rem 1.05rem;border:1px solid #d7e7da;border-radius:20px;
        background:linear-gradient(180deg,#ffffff 0%,#f1f7f2 100%);
        box-shadow:0 10px 30px rgba(22,53,40,0.06);">
            <div style="font-size:0.8rem;color:#577464;text-transform:uppercase;letter-spacing:0.06em;">{label}</div>
            <div style="font-size:1.9rem;font-weight:700;color:#173528;">{value}</div>
            <div style="font-size:0.9rem;color:#2d6a4f;">{delta}</div>
        </div>
        """,
        unsafe_allow_html=True,
    )


@st.cache_data(show_spinner=False)
def load_delta_table(path: Path) -> pd.DataFrame:
    return DeltaTable(str(path)).to_pandas()


@st.cache_data(show_spinner=False)
def load_dashboard_data() -> pd.DataFrame:
    fact_df = load_delta_table(FACT_TRAFFIC_PATH)
    zone_df = load_delta_table(DIM_ZONE_PATH)
    road_df = load_delta_table(DIM_ROAD_PATH)
    coords_df = pd.DataFrame(
        [{"city_zone": zone, **coords} for zone, coords in ZONE_COORDS.items()]
    )

    fact_df["event_ts"] = pd.to_datetime(fact_df["event_ts"], errors="coerce", utc=True)
    fact_df["date"] = pd.to_datetime(fact_df["date"], errors="coerce").dt.date
    fact_df["hour"] = pd.to_numeric(fact_df["hour"], errors="coerce")
    fact_df["speed_int"] = pd.to_numeric(fact_df["speed_int"], errors="coerce")
    fact_df["congestion_level"] = pd.to_numeric(fact_df["congestion_level"], errors="coerce")
    fact_df["peak_flag"] = pd.to_numeric(fact_df["peak_flag"], errors="coerce").fillna(0)

    merged = fact_df.merge(zone_df, on="city_zone", how="left")
    merged = merged.merge(road_df, on="road_id", how="left")
    merged = merged.merge(coords_df, on="city_zone", how="left")

    merged["peak_label"] = merged["peak_flag"].map({1: "Peak", 0: "Off-Peak"}).fillna("Unknown")
    merged["speed_band"] = merged["speed_band"].fillna("UNKNOWN")
    merged["weather"] = merged["weather"].fillna("UNKNOWN")

    return merged


def ensure_gold_tables() -> None:
    missing_paths = [
        str(path.relative_to(ROOT_DIR))
        for path in [FACT_TRAFFIC_PATH, DIM_ZONE_PATH, DIM_ROAD_PATH]
        if not path.exists()
    ]

    if missing_paths:
        st.error(
            "Gold data is missing. Run the gold layer first.\n\nMissing paths:\n- "
            + "\n- ".join(missing_paths)
        )
        st.stop()


def render_sidebar_filters(data: pd.DataFrame) -> pd.DataFrame:
    st.sidebar.header("Control Room")

    if st.sidebar.button("Refresh Delta Data", use_container_width=True):
        st.cache_data.clear()
        st.rerun()

    available_dates = sorted(d for d in data["date"].dropna().unique())
    default_dates = (
        (available_dates[0], available_dates[-1])
        if available_dates
        else ()
    )

    date_range = st.sidebar.date_input("Date Range", value=default_dates)
    selected_zones = st.sidebar.multiselect(
        "City Zone",
        options=sorted(data["city_zone"].dropna().unique()),
    )
    selected_roads = st.sidebar.multiselect(
        "Road",
        options=sorted(data["road_id"].dropna().unique()),
    )
    selected_weather = st.sidebar.multiselect(
        "Weather",
        options=sorted(data["weather"].dropna().unique()),
    )
    selected_peak = st.sidebar.multiselect(
        "Peak Window",
        options=sorted(data["peak_label"].dropna().unique()),
    )

    filtered = data.copy()

    if len(date_range) == 2:
        start_date, end_date = date_range
        filtered = filtered[filtered["date"].between(start_date, end_date)]

    if selected_zones:
        filtered = filtered[filtered["city_zone"].isin(selected_zones)]

    if selected_roads:
        filtered = filtered[filtered["road_id"].isin(selected_roads)]

    if selected_weather:
        filtered = filtered[filtered["weather"].isin(selected_weather)]

    if selected_peak:
        filtered = filtered[filtered["peak_label"].isin(selected_peak)]

    return filtered


def build_zone_map(zone_summary: pd.DataFrame) -> pdk.Deck:
    map_df = zone_summary.copy()
    map_df["radius"] = map_df["events"].fillna(0).clip(lower=1) * 350
    map_df["red"] = (map_df["avg_congestion"].fillna(0) * 42).clip(0, 255)
    map_df["green"] = (220 - map_df["avg_congestion"].fillna(0) * 25).clip(50, 220)
    map_df["blue"] = 105

    layer = pdk.Layer(
        "ScatterplotLayer",
        data=map_df,
        get_position="[lon, lat]",
        get_fill_color="[red, green, blue, 190]",
        get_line_color=[18, 53, 40, 220],
        get_radius="radius",
        pickable=True,
        stroked=True,
        line_width_min_pixels=2,
    )

    view_state = pdk.ViewState(
        latitude=map_df["lat"].mean(),
        longitude=map_df["lon"].mean(),
        zoom=10.2,
        pitch=35,
    )

    tooltip = {
        "html": """
        <b>{city_zone}</b><br/>
        Events: {events}<br/>
        Avg speed: {avg_speed}<br/>
        Avg congestion: {avg_congestion}<br/>
        Zone type: {zone_type}<br/>
        Risk: {traffic_risk}
        """,
        "style": {"backgroundColor": "#163528", "color": "white"},
    }

    return pdk.Deck(
        map_style="mapbox://styles/mapbox/light-v11",
        initial_view_state=view_state,
        layers=[layer],
        tooltip=tooltip,
    )
