CREATE DATABASE IF NOT EXISTS mobility;

USE mobility;

CREATE TABLE IF NOT EXISTS fact_traffic
USING DELTA
LOCATION '/opt/spark/warehouse/fact_traffic';

CREATE TABLE IF NOT EXISTS dim_zone
USING DELTA
LOCATION '/opt/spark/warehouse/dim_zone';

CREATE TABLE IF NOT EXISTS dim_road
USING DELTA
LOCATION '/opt/spark/warehouse/dim_road';

CREATE OR REPLACE VIEW bi_fact_traffic AS
SELECT
  CAST(vehicle_id AS STRING) AS vehicle_id,
  CAST(road_id AS STRING) AS road_id,
  CAST(city_zone AS STRING) AS city_zone,
  CAST(speed_int AS DOUBLE) AS speed,
  CAST(congestion_level AS INT) AS congestion_level,
  CAST(event_ts AS TIMESTAMP) AS event_time,
  CAST(peak_flag AS STRING) AS peak_flag,
  CAST(speed_band AS STRING) AS speed_band,
  CAST(weather AS STRING) AS weather,
  CAST(date AS DATE) AS event_date,
  CAST(hour AS INT) AS event_hour
FROM fact_traffic;

CREATE OR REPLACE VIEW bi_dim_zone AS
SELECT
  CAST(city_zone AS STRING) AS city_zone,
  CAST(zone_type AS STRING) AS zone_type,
  CAST(traffic_risk AS STRING) AS traffic_risk
FROM dim_zone;

CREATE OR REPLACE VIEW bi_dim_road AS
SELECT
  CAST(road_id AS STRING) AS road_id,
  CAST(road_type AS STRING) AS road_type,
  CAST(speed_limit AS INT) AS speed_limit
FROM dim_road;

SELECT COUNT(*) AS fact_rows FROM fact_traffic;
SELECT COUNT(*) AS zone_rows FROM dim_zone;
SELECT COUNT(*) AS road_rows FROM dim_road;
