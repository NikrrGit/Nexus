import plotly.express as px
import streamlit as st

from dashboard.shared import (
    build_zone_map,
    ensure_gold_tables,
    inject_theme,
    load_dashboard_data,
    metric_card,
    render_sidebar_filters,
)


st.set_page_config(page_title="Mobility Dashboard", layout="wide")
inject_theme()
ensure_gold_tables()

data = load_dashboard_data()
filtered = render_sidebar_filters(data)

if filtered.empty:
    st.warning("No rows match the current filters.")
    st.stop()

st.markdown(
    """
    <div class="hero-card">
        <div class="hero-eyebrow">Urban Mobility Command Center</div>
        <div class="hero-title">Traffic performance, congestion risk, and zone activity in one place.</div>
        <div class="hero-copy">
            This dashboard reads the gold Delta tables directly and turns them into an operations-friendly view for
            hourly traffic, hotspot monitoring, road usage, and weather impact.
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

avg_speed = filtered["speed_int"].mean()
avg_congestion = filtered["congestion_level"].mean()
peak_share = filtered["peak_flag"].eq(1).mean() * 100
high_risk_share = filtered["traffic_risk"].eq("HIGH").mean() * 100

metric_cols = st.columns(4)
with metric_cols[0]:
    metric_card("Traffic Events", f"{len(filtered):,}", "Live filtered event count")
with metric_cols[1]:
    metric_card("Average Speed", f"{avg_speed:.1f}", "Across visible traffic records")
with metric_cols[2]:
    metric_card("Average Congestion", f"{avg_congestion:.1f}", "Mean congestion score")
with metric_cols[3]:
    metric_card("High Risk Share", f"{high_risk_share:.1f}%", f"Peak traffic share {peak_share:.1f}%")

zone_summary = (
    filtered.groupby(
        ["city_zone", "zone_type", "traffic_risk", "lat", "lon"],
        dropna=False,
    )
    .agg(
        events=("vehicle_id", "count"),
        avg_speed=("speed_int", "mean"),
        avg_congestion=("congestion_level", "mean"),
    )
    .reset_index()
    .sort_values("events", ascending=False)
)
zone_summary["avg_speed"] = zone_summary["avg_speed"].round(1)
zone_summary["avg_congestion"] = zone_summary["avg_congestion"].round(1)

hourly_df = (
    filtered.groupby("hour", dropna=False)
    .size()
    .reset_index(name="events")
    .sort_values("hour")
)

road_df = (
    filtered.groupby(["road_id", "road_type"], dropna=False)
    .agg(
        events=("vehicle_id", "count"),
        avg_speed=("speed_int", "mean"),
        avg_congestion=("congestion_level", "mean"),
    )
    .reset_index()
    .sort_values("events", ascending=False)
)
road_df["avg_speed"] = road_df["avg_speed"].round(1)
road_df["avg_congestion"] = road_df["avg_congestion"].round(1)

weather_df = (
    filtered.groupby("weather", dropna=False)
    .agg(
        events=("vehicle_id", "count"),
        avg_speed=("speed_int", "mean"),
        avg_congestion=("congestion_level", "mean"),
    )
    .reset_index()
    .sort_values("events", ascending=False)
)

speed_band_df = (
    filtered.groupby("speed_band", dropna=False)
    .size()
    .reset_index(name="events")
    .sort_values("events", ascending=False)
)

map_col, zone_col = st.columns([1.25, 1])

with map_col:
    st.markdown('<div class="section-title">Zone Activity Map</div>', unsafe_allow_html=True)
    st.pydeck_chart(build_zone_map(zone_summary), use_container_width=True)
    st.caption("Map positions are representative zone centroids for the synthetic dataset.")

with zone_col:
    st.markdown('<div class="section-title">Zone Performance</div>', unsafe_allow_html=True)
    zone_chart = px.bar(
        zone_summary,
        x="city_zone",
        y="avg_congestion",
        color="traffic_risk",
        hover_data=["events", "avg_speed", "zone_type"],
        title="Congestion by Zone",
        color_discrete_sequence=["#40916c", "#e9c46a", "#d62828"],
    )
    zone_chart.update_layout(
        title_x=0.02,
        xaxis_title="Zone",
        yaxis_title="Average Congestion",
        legend_title="Risk",
    )
    st.plotly_chart(zone_chart, use_container_width=True)

chart_col_1, chart_col_2 = st.columns(2)

with chart_col_1:
    st.markdown('<div class="section-title">Hourly Traffic Pulse</div>', unsafe_allow_html=True)
    hourly_chart = px.area(
        hourly_df,
        x="hour",
        y="events",
        markers=True,
        title="Traffic Events by Hour",
        color_discrete_sequence=["#2d6a4f"],
    )
    hourly_chart.update_layout(title_x=0.02, xaxis_title="Hour", yaxis_title="Events")
    st.plotly_chart(hourly_chart, use_container_width=True)

with chart_col_2:
    st.markdown('<div class="section-title">Top Roads</div>', unsafe_allow_html=True)
    road_chart = px.bar(
        road_df.head(10),
        x="road_id",
        y="events",
        color="road_type",
        hover_data=["avg_speed", "avg_congestion"],
        title="Highest Traffic Roads",
        color_discrete_sequence=["#1d3557", "#457b9d"],
    )
    road_chart.update_layout(title_x=0.02, xaxis_title="Road", yaxis_title="Events")
    st.plotly_chart(road_chart, use_container_width=True)

chart_col_3, chart_col_4 = st.columns(2)

with chart_col_3:
    st.markdown('<div class="section-title">Speed Mix</div>', unsafe_allow_html=True)
    speed_band_chart = px.pie(
        speed_band_df,
        names="speed_band",
        values="events",
        hole=0.55,
        title="Speed Band Distribution",
        color_discrete_sequence=["#74c69d", "#40916c", "#1b4332"],
    )
    speed_band_chart.update_layout(title_x=0.02)
    st.plotly_chart(speed_band_chart, use_container_width=True)

with chart_col_4:
    st.markdown('<div class="section-title">Weather Impact</div>', unsafe_allow_html=True)
    weather_chart = px.scatter(
        weather_df,
        x="avg_speed",
        y="avg_congestion",
        size="events",
        color="weather",
        hover_name="weather",
        title="Weather vs Speed and Congestion",
        color_discrete_sequence=px.colors.qualitative.Safe,
    )
    weather_chart.update_layout(title_x=0.02, xaxis_title="Average Speed", yaxis_title="Average Congestion")
    st.plotly_chart(weather_chart, use_container_width=True)

st.markdown('<div class="section-title">Operational Feed</div>', unsafe_allow_html=True)
preview_columns = [
    "event_ts",
    "vehicle_id",
    "city_zone",
    "road_id",
    "road_type",
    "speed_int",
    "congestion_level",
    "weather",
    "traffic_risk",
    "peak_label",
]
st.dataframe(
    filtered[preview_columns].sort_values("event_ts", ascending=False),
    use_container_width=True,
    hide_index=True,
)
