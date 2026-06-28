# Running the POC

## How to run

```bash
./run_poc.sh
```

No arguments required.

---

## Service Endpoints

Quick reference for all services started by `docker compose up`.

| Service | URL / Address | Credentials | Purpose |
|---|---|---|---|
| **Kafka** | `localhost:9092` | — | Broker (PLAINTEXT) |
| **Zookeeper** | `localhost:2181` | — | Kafka coordination |
| **Kafka Connect S3 (Iceberg Sink)** | http://localhost:8084 | — | Iceberg sink connector REST API |
| **MinIO API** | http://localhost:9000 | `minioadmin` / `minioadmin` | S3-compatible object storage |
| **MinIO Console** | http://localhost:9001 | `minioadmin` / `minioadmin` | Web UI for browsing buckets |
| **ClickHouse HTTP** | http://localhost:8123 | `default` / _(no password)_ | OLAP query HTTP interface |
| **ClickHouse TCP** | `localhost:19000` | `default` / _(no password)_ | OLAP native TCP port |
| **Flink JobManager UI** | http://localhost:8081 | — | Flink dashboard (submit/monitor jobs) |
| **Hive Metastore** | `localhost:9083` (Thrift) | — | Iceberg/Trino catalog metadata |
| **Trino** | http://localhost:8080 | — | Federated SQL over Gold Iceberg tables |
| **Spark Master UI** | http://localhost:8082 | — | Spark standalone cluster dashboard |
| **Spark Master (cluster)** | `spark://localhost:7077` | — | spark-submit endpoint |
| **Cube** | http://localhost:4000 | API secret: `dev-secret-change-in-prod` | Semantic layer REST & GraphQL API |

> **Cube** is not started by `docker compose up` — run it separately (see [Cube Semantic Layer](#cube-semantic-layer--querying-gold-via-rest-api)).

---

## Prerequisites

The following tools must be installed and on your `PATH`:

| Tool | Version | Purpose |
|---|---|---|
| Docker + Docker Compose | 24+ | All services (Kafka, MinIO, ClickHouse, Flink, Spark, etc.) |
| Java (JDK) | 11 | Building Flink and Spark JARs |
| Maven | 3.8+ | Building Flink and Spark JARs |
| Python | 3.10+ | Market data download script |

> **Spark runs inside Docker** (`bitnami/spark:3.5`). No local Spark installation is required; `run_poc.sh` submits jobs via `docker exec spark-master spark-submit`.

Verify:
```bash
docker --version && docker compose version
java -version
mvn -version
python3 --version
```

---

## Build (one-time setup)

Run these steps once before starting the pipeline for the first time, or after code changes.

### 1. Install Python dependencies

Required for the market-data download script.

```bash
pip install -r requirements.txt
```

### 2. Build the Kafka Connect Docker image

The `connect-s3` service uses a custom image that bundles the Iceberg Kafka Connect runtime. Build it before starting services:

```bash
docker compose build kafka-connect-s3
```

### 3. Build the Flink JAR

```bash
cd flink_processing && mvn clean package -q && cd ..
```

Output: `flink_processing/target/flink-processing-1.0-SNAPSHOT.jar`

### 4. Build the Spark JAR

```bash
cd spark_processing && mvn clean package -q && cd ..
```

Output: `spark_processing/target/spark-processing-1.0-SNAPSHOT.jar`

---

## Full pipeline — step by step

Run these in order to go from a cold start to queryable gold Iceberg tables.

### Step 1 — Start services and seed Kafka

```bash
./run_poc.sh
```

This single command:
- Starts all Docker services (Zookeeper, Kafka, MinIO, Kafka Connect, ClickHouse, Spark master + worker)
- Creates the `warehouse/bronze`, `warehouse/silver`, `warehouse/gold` prefixes in MinIO
- Creates ClickHouse tables (`ohlcv_bars`, `vwap_daily`) if they don't exist
- Resets and recreates the `market-data` Kafka topic
- Registers the Iceberg Sink connector and waits for it to reach `RUNNING`
- Pushes all CSV files from `data/` into the `market-data` topic as JSON messages
- Waits 65 s for the Iceberg sink to commit the first bronze snapshot
- Submits `BronzeToSilverJob` to the Spark standalone cluster
- Submits `SilverToGoldJob` to the Spark standalone cluster

### Step 2 — Register the Iceberg Sink connector

The connector is registered automatically by `run_poc.sh`. To verify it is running:

```bash
curl -s http://localhost:8084/connectors/iceberg-sink-market-data/status
```

Bronze data lands at `s3://warehouse/bronze/market_data/`.

### Steps 3 & 4 — Spark Bronze → Silver → Gold (automatic)

> **Note (WSL2 / Docker Desktop):** Volume mounts from the WSL2 Linux filesystem fail silently in Docker Desktop. Copy the JAR directly into the container via `docker cp` before each run.

```bash
# Copy the JAR into the spark-master container (required after every mvn package)
docker cp spark_processing/target/spark-processing-1.0-SNAPSHOT.jar spark-master:/tmp/

# Bronze → Silver
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --class com.retailbroking.BronzeToSilverJob \
  /tmp/spark-processing-1.0-SNAPSHOT.jar

# Silver → Gold
docker exec spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --class com.retailbroking.SilverToGoldJob \
  /tmp/spark-processing-1.0-SNAPSHOT.jar
```

Monitor progress at the **Spark Master UI**: http://localhost:8082

Verify output in MinIO:
```bash
docker run --rm --network container:minio --entrypoint /bin/sh minio/mc:latest -c \
  "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1 && \
   mc ls --recursive local/warehouse/silver/ && \
   mc ls --recursive local/warehouse/gold/"
```

### Step 5 — Query the gold tables via Trino

Trino exposes the Gold Iceberg tables over SQL. Connect to the Trino CLI running inside the coordinator container and query the `iceberg.gold` schema directly.

**Open an interactive Trino session:**
```bash
docker exec -it trino-coordinator trino --server http://localhost:8080 --catalog iceberg --schema gold
```

#### List available gold tables
```sql
SHOW TABLES IN iceberg.gold;
```

#### Daily summary — all symbols
```sql
SELECT symbol, event_date, open, high, low, close, volume, vwap, daily_return_pct
FROM iceberg.gold.daily_summary
ORDER BY symbol, event_date
LIMIT 50;
```

#### 5-minute OHLCV bars for a symbol
```sql
SELECT symbol, window_start, window_end, open, high, low, close, volume
FROM iceberg.gold.ohlcv_5min
WHERE symbol = 'HDFCBANK.NS'
ORDER BY window_start
LIMIT 20;
```


> **Trino UI** is available at http://localhost:8080 — running queries and worker status are visible there. (user: trino)

### Step 6 — Run the Flink streaming job (optional)

The Flink job consumes `market-data` in real time and computes rolling OHLCV bars
and VWAP. Skip this step if you only need batch results via Spark.

```bash
docker cp flink_processing/target/flink-processing-1.0-SNAPSHOT.jar flink-jobmanager:/tmp/
docker exec flink-jobmanager flink run /tmp/flink-processing-1.0-SNAPSHOT.jar
# Tail the results (currently prints to stdout)
docker logs -f flink-taskmanager
```

### Step 7 — Query the results

**ClickHouse** (OHLCV bars from Flink):
```bash
docker exec -it clickhouse clickhouse-client --database retail_broking \
  --query "SELECT symbol, interval, window_start, open, high, low, close, volume
           FROM ohlcv_bars ORDER BY symbol, window_start LIMIT 20"
```


### Interactive shell
```bash
docker exec -it clickhouse clickhouse-client --database retail_broking
```
