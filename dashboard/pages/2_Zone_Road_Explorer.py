import plotly.express as px
import streamlit as st

from dashboard.shared import ensure_gold_tables, inject_theme, load_dashboard_data, render_sidebar_filters


st.set_page_config(page_title="Zone and Road Explorer", layout="wide")
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
        <div class="hero-eyebrow">Drilldown Workspace</div>
        <div class="hero-title">Compare zones, inspect roads, and understand where congestion builds.</div>
        <div class="hero-copy">
            This page focuses on detailed operational breakdowns so you can move from dashboard summary to targeted action.
        </div>
    </div>
    """,
    unsafe_allow_html=True,
)

selected_zone = st.selectbox(
    "Focus Zone",
    options=sorted(filtered["city_zone"].dropna().unique()),
)

zone_slice = filtered[filtered["city_zone"] == selected_zone].copy()

if zone_slice.empty:
    st.warning("No rows found for the selected zone.")
    st.stop()

zone_metrics = st.columns(3)
with zone_metrics[0]:
    st.metric("Zone Events", f"{len(zone_slice):,}")
with zone_metrics[1]:
    st.metric("Zone Avg Speed", f"{zone_slice['speed_int'].mean():.1f}")
with zone_metrics[2]:
    st.metric("Zone Avg Congestion", f"{zone_slice['congestion_level'].mean():.1f}")

road_mix = (
    zone_slice.groupby(["road_id", "road_type"], dropna=False)
    .agg(
        events=("vehicle_id", "count"),
        avg_speed=("speed_int", "mean"),
        avg_congestion=("congestion_level", "mean"),
    )
    .reset_index()
    .sort_values("events", ascending=False)
)

weather_mix = (
    zone_slice.groupby("weather", dropna=False)
    .agg(
        events=("vehicle_id", "count"),
        avg_speed=("speed_int", "mean"),
        avg_congestion=("congestion_level", "mean"),
    )
    .reset_index()
    .sort_values("events", ascending=False)
)

hour_zone = (
    zone_slice.groupby("hour", dropna=False)
    .size()
    .reset_index(name="events")
    .sort_values("hour")
)

road_compare = (
    filtered.groupby(["city_zone", "road_id"], dropna=False)
    .size()
    .reset_index(name="events")
)

chart_col_1, chart_col_2 = st.columns(2)

with chart_col_1:
    road_chart = px.bar(
        road_mix,
        x="road_id",
        y="events",
        color="avg_congestion",
        hover_data=["road_type", "avg_speed"],
        title=f"Road Pressure in {selected_zone}",
        color_continuous_scale="YlOrRd",
    )
    road_chart.update_layout(title_x=0.02, xaxis_title="Road", yaxis_title="Events")
    st.plotly_chart(road_chart, use_container_width=True)

with chart_col_2:
    zone_hour_chart = px.line(
        hour_zone,
        x="hour",
        y="events",
        markers=True,
        title=f"Hourly Pattern in {selected_zone}",
        color_discrete_sequence=["#2d6a4f"],
    )
    zone_hour_chart.update_layout(title_x=0.02, xaxis_title="Hour", yaxis_title="Events")
    st.plotly_chart(zone_hour_chart, use_container_width=True)

chart_col_3, chart_col_4 = st.columns(2)

with chart_col_3:
    weather_chart = px.bar(
        weather_mix,
        x="weather",
        y="avg_congestion",
        color="events",
        hover_data=["avg_speed"],
        title=f"Weather Stress in {selected_zone}",
        color_continuous_scale="Emrld",
    )
    weather_chart.update_layout(title_x=0.02, xaxis_title="Weather", yaxis_title="Average Congestion")
    st.plotly_chart(weather_chart, use_container_width=True)

with chart_col_4:
    heatmap = px.density_heatmap(
        road_compare,
        x="road_id",
        y="city_zone",
        z="events",
        histfunc="sum",
        title="Zone to Road Demand Heatmap",
        color_continuous_scale="Tealgrn",
    )
    heatmap.update_layout(title_x=0.02, xaxis_title="Road", yaxis_title="Zone")
    st.plotly_chart(heatmap, use_container_width=True)

st.markdown('<div class="section-title">Road Leaderboard</div>', unsafe_allow_html=True)
st.dataframe(
    road_mix.sort_values(["events", "avg_congestion"], ascending=[False, False]),
    use_container_width=True,
    hide_index=True,
)
