#!/usr/bin/env bash
set -euo pipefail

PROJECT_ROOT="$(cd "$(dirname "$0")/.." && pwd)"
WAREHOUSE="$PROJECT_ROOT/warehouse"

SPARK_PACKAGES="io.delta:delta-spark_2.12:3.2.0,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1"
SPARK_SUBMIT_BASE=(
  /opt/spark/bin/spark-submit
  --master spark://spark-master:7077
  --packages "$SPARK_PACKAGES"
  --conf spark.jars.ivy=/tmp/.ivy
  --conf spark.sql.extensions=io.delta.sql.DeltaSparkSessionExtension
  --conf spark.sql.catalog.spark_catalog=org.apache.spark.sql.delta.catalog.DeltaCatalog
)

PRODUCER_DURATION=${PRODUCER_DURATION:-40}
STREAM_TIMEOUT=${STREAM_TIMEOUT:-300}
POLL_INTERVAL=10

info()  { echo -e "\n\033[1;32m==> $1\033[0m"; }
warn()  { echo -e "\033[1;33m    $1\033[0m"; }
fail()  { echo -e "\033[1;31m[ERROR] $1\033[0m" >&2; exit 1; }

wait_for_delta() {
  local table_path="$1" label="$2" elapsed=0
  while [ $elapsed -lt $STREAM_TIMEOUT ]; do
    if [ -d "$table_path/_delta_log" ]; then
      local count
      count=$(find "$table_path" -name "*.parquet" -size +0c 2>/dev/null | wc -l | tr -d ' ')
      if [ "$count" -gt 0 ]; then
        echo "    $label committed ($count parquet files)."
        return 0
      fi
    fi
    sleep $POLL_INTERVAL
    elapsed=$((elapsed + POLL_INTERVAL))
    echo "    Waiting for $label... (${elapsed}s)"
  done
  fail "$label did not commit within ${STREAM_TIMEOUT}s."
}

kill_spark_app() {
  local app_name="$1"
  docker exec spark-master bash -c \
    "ps aux | grep '$app_name' | grep -v grep | awk '{print \$2}' | xargs -r kill -9" 2>/dev/null || true
  sleep 3
}

# ── 0. Preflight ────────────────────────────────────────────────────
info "Preflight: checking Docker containers"
for svc in spark-master spark-worker-1 spark-worker-2 kafka dbt-warehouse-db; do
  docker inspect --format='{{.State.Running}}' "$svc" 2>/dev/null | grep -q true \
    || fail "Container '$svc' is not running. Run: docker compose up -d"
done
echo "    All required containers are healthy."

# ── 1. Clean previous run (optional) ───────────────────────────────
if [ "${CLEAN:-0}" = "1" ]; then
  info "Cleaning previous warehouse data"
  rm -rf "$WAREHOUSE/traffic_bronze" "$WAREHOUSE/traffic_silver" \
         "$WAREHOUSE/dim_zone" "$WAREHOUSE/dim_road" "$WAREHOUSE/fact_traffic" \
         "$WAREHOUSE/chk"
  echo "    Warehouse cleaned."
fi

# ── 2. Bronze: Kafka → Delta Bronze ────────────────────────────────
info "Starting Bronze stream (Kafka → Delta Bronze)"
docker exec spark-master "${SPARK_SUBMIT_BASE[@]}" \
  /opt/spark-apps/traffic_bronze.py > /tmp/nexus_bronze.log 2>&1 &
echo "    Bronze stream launched. Waiting for Kafka consumer to initialise..."
sleep 60

info "Starting traffic producer (${PRODUCER_DURATION}s)"
cd "$PROJECT_ROOT"
python3 -c "
import subprocess, time, signal, sys
proc = subprocess.Popen([sys.executable, 'producer/traffic_data_producer.py'],
                        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
time.sleep(${PRODUCER_DURATION})
proc.send_signal(signal.SIGINT)
try:
    proc.wait(timeout=5)
except subprocess.TimeoutExpired:
    proc.kill()
print('    Producer stopped.')
"

info "Waiting for Bronze to commit"
wait_for_delta "$WAREHOUSE/traffic_bronze" "Bronze"
kill_spark_app "traffic_bronze"
echo "    Bronze stream killed."

# ── 3. Silver: Delta Bronze → Delta Silver ─────────────────────────
info "Starting Silver stream (Bronze → Silver)"
docker exec spark-master "${SPARK_SUBMIT_BASE[@]}" \
  /opt/spark-apps/traffic_silver.py > /tmp/nexus_silver.log 2>&1 &

wait_for_delta "$WAREHOUSE/traffic_silver" "Silver"
kill_spark_app "traffic_silver"
echo "    Silver stream killed."

# ── 4. Gold: Delta Silver → Star Schema ────────────────────────────
info "Running Gold layer (Silver → dim_zone, dim_road, fact_traffic)"
docker exec spark-master "${SPARK_SUBMIT_BASE[@]}" \
  /opt/spark-apps/traffic_gold.py > /tmp/nexus_gold.log 2>&1
echo "    Gold layer complete (auto-terminated)."

# ── 5. Load Gold Delta → Postgres ──────────────────────────────────
info "Loading Gold tables into Postgres"
cd "$PROJECT_ROOT"
python3 scripts/load_gold_delta_to_psql.py
echo "    Postgres staging tables loaded."

# ── 6. dbt: build warehouse models + tests ─────────────────────────
info "Running dbt (deps → run → test)"
cd "$PROJECT_ROOT/dbt"
dbt deps   --quiet
dbt run    --quiet
dbt test   --quiet
echo "    dbt models built and all tests passed."

# ── 7. Dashboard ────────────────────────────────────────────────────
info "Pipeline complete! Starting Streamlit dashboard..."
cd "$PROJECT_ROOT"
echo "    Open http://localhost:8501 in your browser."
echo "    Press Ctrl+C to stop."
streamlit run dashboard/app.py
