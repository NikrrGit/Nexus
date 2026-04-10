select
    f.vehicle_id,
    f.road_id,
    f.city_zone,
    f.speed_int,
    f.congestion_level,
    f.event_ts,
    f.peak_flag,
    f.speed_band,
    f.hour,
    f.weather,
    f.date
from {{ ref('fct_traffic_events') }} f