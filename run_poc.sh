#!/bin/bash

set -e

# ─── Config ───────────────────────────────────────────────────────────────────

SERVICES=("zookeeper" "kafka" "minio" "hive-metastore" "trino" "kafka-connect-s3" "clickhouse" "spark-master" "spark-worker")

KAFKA_TOPICS=("market-data" "control-iceberg")
KAFKA_CONTAINER="kafka"
KAFKA_BOOTSTRAP="localhost:9092"

DATA_DIR="$(cd "$(dirname "$0")" && pwd)/data"

CLICKHOUSE_URL="http://localhost:8123"
CLICKHOUSE_DB="retail_broking"

# ─── Helpers ──────────────────────────────────────────────────────────────────

log()  { echo "==> $*"; }
info() { echo "    $*"; }

# ─── Service Management ───────────────────────────────────────────────────────

is_service_running() {
  local name=$1
  local status
  status=$(docker inspect -f '{{.State.Running}}' "$name" 2>/dev/null)
  [[ "$status" == "true" ]]
}

check_services() {
  log "Checking services..."
  local all_up=true

  for svc in "${SERVICES[@]}"; do
    if is_service_running "$svc"; then
      info "[up]   $svc"
    else
      info "[down] $svc"
      all_up=false
    fi
  done

  echo ""
  if [ "$all_up" = false ]; then
    start_services
  else
    log "All services are already running."
  fi
}

start_services() {
  log "Starting services via docker compose..."
  docker compose up -d --build
  echo ""

  log "Waiting 10 seconds for services to become healthy..."
  sleep 10

  local failed=false
  for svc in "${SERVICES[@]}"; do
    if is_service_running "$svc"; then
      info "[up]   $svc"
    else
      info "[FAILED] $svc did not start — check: docker logs $svc"
      failed=true
    fi
  done

  if [ "$failed" = true ]; then
    echo ""
    log "ERROR: One or more services failed to start. Aborting."
    exit 1
  fi
}

# ─── MinIO Paths ──────────────────────────────────────────────────────────────

ensure_minio_paths() {
  echo ""
  log "Ensuring MinIO warehouse path (bronze)..."
  # silver and gold are created automatically by Spark as silver.db/ and gold.db/
  # (Hive catalog default: {warehouse}/{namespace}.db/). No placeholder needed.

  docker run --rm --network container:minio --entrypoint /bin/sh minio/mc:latest -c "
    mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1
    if mc stat local/warehouse/bronze/.keep >/dev/null 2>&1; then
      echo '    [exists]  warehouse/bronze/'
    else
      printf '' | mc pipe local/warehouse/bronze/.keep >/dev/null 2>&1
      echo '    [created] warehouse/bronze/'
    fi
  "
}

# ─── ClickHouse Tables ────────────────────────────────────────────────────────

ch_query() {
  curl -s "${CLICKHOUSE_URL}/?database=${CLICKHOUSE_DB}" --data-binary "$1"
}

ensure_clickhouse_tables() {
  echo ""
  log "Ensuring ClickHouse tables..."

  local attempts=0
  until curl -sf "${CLICKHOUSE_URL}/ping" | grep -q "Ok"; do
    if [ "$attempts" -ge 30 ]; then
      log "ERROR: ClickHouse HTTP interface not ready after 60s."
      exit 1
    fi
    sleep 2
    ((attempts++)) || true
  done
  info "[ready] ClickHouse HTTP interface"

  local err

  err=$(ch_query "
    CREATE TABLE IF NOT EXISTS ohlcv_bars (
      symbol       LowCardinality(String),
      interval     LowCardinality(String),
      window_start DateTime,
      window_end   DateTime,
      open         Float64,
      high         Float64,
      low          Float64,
      close        Float64,
      volume       UInt64
    ) ENGINE = MergeTree()
    PARTITION BY symbol
    ORDER BY (symbol, interval, window_start)
  ")
  if [ -n "$err" ]; then
    info "[ERROR] ohlcv_bars: $err"; exit 1
  fi
  info "[ok] ohlcv_bars  (partitioned by symbol, ordered by symbol, interval, window_start)"

  err=$(ch_query "
    CREATE TABLE IF NOT EXISTS vwap_daily (
      symbol       LowCardinality(String),
      date         Date,
      vwap         Float64,
      total_volume UInt64
    ) ENGINE = MergeTree()
    PARTITION BY symbol
    ORDER BY (symbol, date)
  ")
  if [ -n "$err" ]; then
    info "[ERROR] vwap_daily: $err"; exit 1
  fi
  info "[ok] vwap_daily  (partitioned by symbol, ordered by symbol, date)"
}

# ─── Kafka Topics ─────────────────────────────────────────────────────────────

topic_exists() {
  local topic=$1
  docker exec "$KAFKA_CONTAINER" \
    kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" --list 2>/dev/null \
    | grep -qx "$topic"
}

delete_topic() {
  local topic=$1
  docker exec "$KAFKA_CONTAINER" \
    kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --delete --topic "$topic" > /dev/null 2>&1 || true

  local attempts=0
  while topic_exists "$topic" && [ "$attempts" -lt 10 ]; do
    sleep 1
    ((attempts++)) || true
  done
}

create_topic() {
  local topic=$1
  docker exec "$KAFKA_CONTAINER" \
    kafka-topics --bootstrap-server "$KAFKA_BOOTSTRAP" \
    --create --topic "$topic" \
    --partitions 1 --replication-factor 1 \
    --if-not-exists \
    > /dev/null 2>&1
}

reset_kafka_topics() {
  echo ""
  log "Resetting Kafka topics..."

  for topic in "${KAFKA_TOPICS[@]}"; do
    if topic_exists "$topic"; then
      info "[deleting] $topic..."
      delete_topic "$topic"
    fi
    info "[creating] $topic (keyed by symbol)..."
    create_topic "$topic"
    info "[ready]    $topic"
  done
}

# ─── Iceberg Sink Connector ───────────────────────────────────────────────────

register_iceberg_sink() {
  echo ""
  log "Registering Iceberg Sink Connector (market-data → MinIO warehouse/bronze/market_data via Hive Metastore)..."

  local attempts=0
  until curl -sf http://localhost:8084/connectors >/dev/null 2>&1; do
    if [ "$attempts" -ge 30 ]; then
      info "[FAILED] kafka-connect-s3 REST API not reachable after 60s"
      return 1
    fi
    sleep 2
    ((attempts++)) || true
  done

  if curl -sf http://localhost:8084/connectors/iceberg-sink-market-data >/dev/null 2>&1; then
    info "[deleting] existing iceberg-sink-market-data..."
    curl -sf -X DELETE http://localhost:8084/connectors/iceberg-sink-market-data >/dev/null
    sleep 2
  fi

  local trino_attempts=0
  until curl -sf http://localhost:8080/v1/info >/dev/null 2>&1; do
    if [ "$trino_attempts" -ge 30 ]; then
      info "[FAILED] Trino REST API not reachable after 60s"
      return 1
    fi
    sleep 2
    ((trino_attempts++)) || true
  done
  info "[ready] Trino / Hive Metastore catalog"

  docker exec trino trino --execute \
    "CREATE SCHEMA IF NOT EXISTS iceberg.bronze WITH (location = 's3a://warehouse/bronze')" \
    >/dev/null
  info "[ready] Hive namespace: bronze"

  curl -sf -X POST http://localhost:8084/connectors \
    -H "Content-Type: application/json" \
    -d '{
      "name": "iceberg-sink-market-data",
      "config": {
        "connector.class": "org.apache.iceberg.connect.IcebergSinkConnector",
        "tasks.max": "1",
        "topics": "market-data",
        "iceberg.tables": "bronze.market_data",
        "iceberg.tables.auto-create-enabled": "true",
        "iceberg.tables.evolve-schema-enabled": "true",
        "iceberg.tables.schema-force-optional": "true",
        "iceberg.control.topic": "control-iceberg",
        "iceberg.control.commit.interval-ms": "60000",
        "iceberg.control.commit.timeout-ms": "300000",
        "iceberg.catalog.type": "hive",
        "iceberg.catalog.uri": "thrift://hive-metastore:9083",
        "iceberg.catalog.warehouse": "s3a://warehouse/",
        "iceberg.catalog.io-impl": "org.apache.iceberg.aws.s3.S3FileIO",
        "iceberg.catalog.s3.endpoint": "http://minio:9000",
        "iceberg.catalog.s3.access-key-id": "minioadmin",
        "iceberg.catalog.s3.secret-access-key": "minioadmin",
        "iceberg.catalog.s3.path-style-access": "true",
        "iceberg.catalog.client.region": "us-east-1",
        "key.converter": "org.apache.kafka.connect.storage.StringConverter",
        "value.converter": "org.apache.kafka.connect.json.JsonConverter",
        "value.converter.schemas.enable": "false"
      }
    }' >/dev/null

  info "[registered] iceberg-sink-market-data → s3a://warehouse/bronze/market_data/ (Iceberg via Hive Metastore)"
   
  local task_attempts=0
  local task_state=""
  until [ "$task_state" = "RUNNING" ]; do
    if [ "$task_attempts" -ge 30 ]; then
      local trace
      trace=$(curl -sf http://localhost:8084/connectors/iceberg-sink-market-data/status | python3 -c "import sys,json; t=json.load(sys.stdin)['tasks']; print(t[0].get('trace','no trace')) if t else print('no tasks')" 2>/dev/null)
      info "[FAILED] Connector task did not reach RUNNING after 60s"
      info "  trace: $trace"
      return 1
    fi
    sleep 2
    task_state=$(curl -sf http://localhost:8084/connectors/iceberg-sink-market-data/status \
      | python3 -c "import sys,json; t=json.load(sys.stdin)['tasks']; print(t[0]['state']) if t else print('NO_TASKS')" 2>/dev/null)
    ((task_attempts++)) || true
  done
  info "[running] connector task is RUNNING"
}

# ─── Market Data Producer ─────────────────────────────────────────────────────

push_market_data() {
  echo ""
  log "Pushing market data to Kafka topic: market-data..."

  if [ ! -d "$DATA_DIR" ]; then
    info "[skip] data/ directory not found"
    return
  fi

  local found=false
  for csv_file in "$DATA_DIR"/*.csv; do
    [ -f "$csv_file" ] || continue
    found=true

    local filename row_count
    filename=$(basename "$csv_file")
    row_count=$(( $(wc -l < "$csv_file") - 1 ))

    info "Pushing $filename ($row_count rows) -> market-data..."

    tail -n +2 "$csv_file" | awk -F',' '{
      printf "%s:{\"Datetime\":\"%s\",\"Open\":%s,\"High\":%s,\"Low\":%s,\"Close\":%s,\"Volume\":%s,\"Symbol\":\"%s\"}\n",
        $7, $1, $2, $3, $4, $5, $6, $7
    }' | docker exec -i "$KAFKA_CONTAINER" \
      kafka-console-producer --bootstrap-server "$KAFKA_BOOTSTRAP" \
      --topic "market-data" \
      --property "parse.key=true" \
      --property "key.separator=:" > /dev/null 2>&1

    info "[done] $filename"
  done

  if [ "$found" = false ]; then
    info "[skip] No CSV files found in data/"
  fi
}

# ─── Main ─────────────────────────────────────────────────────────────────────

main() {
  check_services
  ensure_minio_paths
  ensure_clickhouse_tables
  reset_kafka_topics
  register_iceberg_sink
  push_market_data

  echo ""
  log "Ready."
}

main
