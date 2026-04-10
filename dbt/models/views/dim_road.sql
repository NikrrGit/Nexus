select distinct
    road_id,
    road_type,
    speed_limit
from {{ ref('dim_locations') }}
