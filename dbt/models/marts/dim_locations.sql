with zones as (
    select * from {{ source('staging', 'stg_dim_zone') }}
),

roads as (
    select * from {{ source('staging', 'stg_dim_road') }}
),

pairs as (
    select distinct city_zone, road_id
    from {{ source('staging', 'stg_fact_traffic') }}
)

select
    md5(p.city_zone || '|' || p.road_id) as location_id,
    p.city_zone,
    z.zone_type,
    z.traffic_risk,
    p.road_id,
    r.road_type,
    r.speed_limit
from pairs p
left join zones z on p.city_zone = z.city_zone
left join roads r on p.road_id = r.road_id
