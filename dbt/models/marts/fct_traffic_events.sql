select
    md5(vehicle_id || '|' || cast(event_ts as text)) as event_id,
    md5(city_zone || '|' || road_id) as location_id,
    vehicle_id,
    road_id,
    city_zone,
    speed_int,
    congestion_level,
    event_ts,
    peak_flag,
    speed_band,
    hour,
    weather,
    date
from {{ source('staging', 'stg_fact_traffic') }}


