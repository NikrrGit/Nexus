select distinct
    city_zone,
    zone_type,
    traffic_risk
from {{ ref('dim_locations') }}
