from __future__ import annotations
import os
from pathlib import Path
import pandas as pd
from deltalake import DeltaTable
from sqlalchemy import create_engine, text

ROOT = Path(__file__).resolve().parents[1]
WAREHOUSE  = ROOT / "warehouse"

def get_pg_url() -> str:
    host = os.getenv("DBT_POSTGRES_HOST", "localhost")
    port = os.getenv("DBT_POSTGRES_PORT", "5436")
    db = os.getenv("DBT_POSTGRES_DB", "warehouse")
    user = os.getenv("DBT_POSTGRES_USER", "dbt")
    password = os.getenv("DBT_POSTGRES_PASSWORD", "dbt")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"

def load_delta(path: Path) -> pd.DataFrame:
    if not path.exists():
        raise FileNotFoundError(f"Missing Delta table path: {path}")
    return DeltaTable(str(path)).to_pandas()


# Normalise the dimensional tables

def normalize_dim_zone(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["city_zone", "zone_type", "traffic_risk"]
    out  = df[cols].copy()
    out["city_zone"] = out["city_zone"].astype("string")
    out["zone_type"] = out["zone_type"].astype("string")
    out["traffic_risk"] = out["traffic_risk"].astype("string")
    return out


def normalize_dim_road(df: pd.DataFrame) -> pd.DataFrame:
    cols = ["road_id", "road_type", "speed_limit"]
    out = df[cols].copy()
    out["road_id"] = out["road_id"].astype("string")
    out["road_type"] = out["road_type"].astype("string")
    out["speed_limit"] = pd.to_numeric(out["speed_limit"], errors="coerce").astype("Int64")
    return out


# Normalise the fact table


def normalize_fact(df: pd.DataFrame) -> pd.DataFrame:
    cols = [
        "vehicle_id", "road_id", "city_zone", "speed_int", "congestion_level",
        "event_ts", "peak_flag", "speed_band", "hour", "weather", "date"
    ]
    out = df[cols].copy()
    out["vehicle_id"] = out["vehicle_id"].astype("string")
    out["road_id"] = out["road_id"].astype("string")
    out["city_zone"] = out["city_zone"].astype("string")
    out["speed_int"] = pd.to_numeric(out["speed_int"], errors="coerce").astype("Int64")
    out["congestion_level"] = pd.to_numeric(out["congestion_level"], errors="coerce").astype("Int64")
    out["event_ts"] = pd.to_datetime(out["event_ts"], errors="coerce", utc=False)
    out["peak_flag"] = pd.to_numeric(out["peak_flag"], errors="coerce").astype("Int64")
    out["speed_band"] = out["speed_band"].astype("string")
    out["hour"] = pd.to_numeric(out["hour"], errors="coerce").astype("Int64")
    out["weather"] = out["weather"].astype("string")
    out["date"] = pd.to_datetime(out["date"], errors="coerce").dt.date
    return out

# staging tables DDL

def create_tables(engine, schema: str) -> None:
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS {schema};
    CREATE TABLE IF NOT EXISTS {schema}.stg_dim_zone (
      city_zone TEXT NOT NULL,
      zone_type TEXT,
      traffic_risk TEXT
    );
    CREATE TABLE IF NOT EXISTS {schema}.stg_dim_road (
      road_id TEXT NOT NULL,
      road_type TEXT,
      speed_limit INTEGER
    );
    CREATE TABLE IF NOT EXISTS {schema}.stg_fact_traffic (
      vehicle_id TEXT NOT NULL,
      road_id TEXT NOT NULL,
      city_zone TEXT NOT NULL,
      speed_int INTEGER,
      congestion_level INTEGER,
      event_ts TIMESTAMP,
      peak_flag INTEGER,
      speed_band TEXT,
      hour INTEGER,
      weather TEXT,
      date DATE
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
def truncate_tables(engine, schema: str) -> None:
    sql = f"""
    TRUNCATE TABLE {schema}.stg_dim_zone;
    TRUNCATE TABLE {schema}.stg_dim_road;
    TRUNCATE TABLE {schema}.stg_fact_traffic;
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
def main() -> None:
    schema = os.getenv("DBT_POSTGRES_SCHEMA", "analytics")
    engine = create_engine(get_pg_url(), future=True)
    dim_zone = normalize_dim_zone(load_delta(WAREHOUSE / "dim_zone"))
    dim_road = normalize_dim_road(load_delta(WAREHOUSE / "dim_road"))
    fact = normalize_fact(load_delta(WAREHOUSE / "fact_traffic"))
    create_tables(engine, schema)
    truncate_tables(engine, schema)
    dim_zone.to_sql("stg_dim_zone", engine, schema=schema, if_exists="append", index=False, method="multi", chunksize=5000)
    dim_road.to_sql("stg_dim_road", engine, schema=schema, if_exists="append", index=False, method="multi", chunksize=5000)
    fact.to_sql("stg_fact_traffic", engine, schema=schema, if_exists="append", index=False, method="multi", chunksize=5000)
    print(f"Loaded rows -> stg_dim_zone={len(dim_zone)}, stg_dim_road={len(dim_road)}, stg_fact_traffic={len(fact)}")


# main function
if __name__ == "__main__":
    main()