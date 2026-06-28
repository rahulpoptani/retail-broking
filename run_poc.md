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
| **Cube** | http://localhost:4000 | API secret: `dev-secret-change-in-prod` | Semantic layer REST & GraphQL API |

> **Cube** is not started by `docker compose up` — run it separately (see [Cube Semantic Layer](#cube-semantic-layer--querying-gold-via-rest-api)).

---

## Prerequisites

The following tools must be installed and on your `PATH`:

| Tool | Version | Purpose |
|---|---|---|
| Docker + Docker Compose | 24+ | All services (Kafka, MinIO, ClickHouse, Flink, etc.) |
| Java (JDK) | 11 | Building Flink and Spark JARs |
| Maven | 3.8+ | Building Flink and Spark JARs |
| Python | 3.10+ | Market data download script |
| Apache Spark | 3.5.x | Running `spark-submit` / `spark-shell` |

Verify:
```bash
docker --version && docker compose version
java -version
mvn -version
python3 --version
spark-submit --version
```

---

## Build (one-time setup)

Run these steps once before starting the pipeline for the first time, or after code changes.

### 1. Install Python dependencies

Required for the market-data download script.

```bash
pip install -r requirements.txt
```

### 2. Download market data

Fetches 1-minute OHLCV bars for `TCS.NS`, `RELIANCE.NS`, and `HDFCBANK.NS` and writes them as CSV files to `data/`.

```bash
python3 scripts/download_data.py
```

Verify:
```bash
ls data/*.csv
```

### 3. Build the Kafka Connect Docker image

The `connect-s3` service uses a custom image that bundles the Iceberg Kafka Connect runtime. Build it before starting services:

```bash
docker compose build kafka-connect-s3
```

### 4. Build the Flink JAR

```bash
cd flink_processing && mvn clean package -q && cd ..
```

Output: `flink_processing/target/flink-processing-1.0-SNAPSHOT.jar`

### 5. Build the Spark JAR

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
- Starts all Docker services (Zookeeper, Kafka, MinIO, Kafka Connect, ClickHouse)
- Creates the `warehouse/bronze`, `warehouse/silver`, `warehouse/gold` prefixes in MinIO
- Creates ClickHouse tables (`ohlcv_bars`, `vwap_daily`) if they don't exist
- Resets and recreates the `market-data` Kafka topic
- Pushes all CSV files from `data/` into the `market-data` topic as JSON messages

### Step 2 — Register the Iceberg Sink connector

After `run_poc.sh` completes, register the Kafka Connect Iceberg Sink so that
messages flow from the `market-data` topic into the bronze Iceberg table.
The connector is registered automatically by the latest version of the script;
verify it is running:

```bash
curl -s http://localhost:8084/connectors/iceberg-sink-market-data/status | jq .state
```

Wait until the status is `RUNNING`. Bronze data lands at `s3://warehouse/bronze/market_data/`.

### Step 3 — (Optional) Run the Flink streaming job

The Flink job consumes `market-data` in real time and computes rolling OHLCV bars
and VWAP. Skip this step if you only need batch results via Spark.

```bash
docker cp flink_processing/target/flink-processing-1.0-SNAPSHOT.jar flink-jobmanager:/tmp/
docker exec flink-jobmanager flink run /tmp/flink-processing-1.0-SNAPSHOT.jar
# Tail the results (currently prints to stdout)
docker logs -f flink-taskmanager
```

### Step 4 — Build the Spark JAR

Skip if already built in the one-time setup above. Rebuild only after code changes.

```bash
cd spark_processing && mvn clean package -q && cd ..
```

### Step 5 — Bronze → Silver (Spark)

Reads the bronze Iceberg table, applies cleaning and type enforcement, and writes
the curated silver Iceberg table partitioned by `(symbol, event_date)`.

```bash
spark-submit \
  --master local[*] \
  --class com.retailbroking.BronzeToSilverJob \
  spark_processing/target/spark-processing-1.0-SNAPSHOT.jar
```

Verify:
```bash
docker run --rm --network container:minio --entrypoint /bin/sh minio/mc:latest -c \
  "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1 && \
   mc ls --recursive local/warehouse/silver/"
```

### Step 6 — Silver → Gold (Spark)

Reads silver and writes four analytical Iceberg tables to gold:
`ohlcv_5min`, `ohlcv_15min`, `ohlcv_1hour`, and `daily_summary`.

```bash
spark-submit \
  --master local[*] \
  --class com.retailbroking.SilverToGoldJob \
  spark_processing/target/spark-processing-1.0-SNAPSHOT.jar
```

Verify:
```bash
docker run --rm --network container:minio --entrypoint /bin/sh minio/mc:latest -c \
  "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1 && \
   mc ls --recursive local/warehouse/gold/"
```

### Step 7 — Query the results

**ClickHouse** (OHLCV bars from Flink):
```bash
docker exec -it clickhouse clickhouse-client --database retail_broking \
  --query "SELECT symbol, interval, window_start, open, high, low, close, volume
           FROM ohlcv_bars ORDER BY symbol, window_start LIMIT 20"
```

**Spark** (gold Iceberg tables):
```bash
spark-shell --master local[*] \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.local=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.local.type=hadoop \
  --conf spark.sql.catalog.local.warehouse=s3a://warehouse/ \
  --conf spark.hadoop.fs.s3a.endpoint=http://localhost:9000 \
  --conf spark.hadoop.fs.s3a.access.key=minioadmin \
  --conf spark.hadoop.fs.s3a.secret.key=minioadmin \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --jars spark_processing/target/spark-processing-1.0-SNAPSHOT.jar
```
```scala
// Inside spark-shell
spark.table("local.gold.daily_summary").orderBy("symbol", "event_date").show(20, truncate = false)
spark.table("local.gold.ohlcv_5min").filter("symbol = 'HDFCBANK.NS'").orderBy("window_start").show(10)
```

---

## What the script does

### 1. Service Check
Checks whether all required Docker services are running:
- `zookeeper`
- `kafka`
- `minio`
- `kafka-connect-s3`
- `clickhouse`

If any service is down, it runs `docker compose up -d` and waits for them to become healthy. If a service still fails to start after that, the script aborts with an error.

### 2. MinIO Paths
Ensures the `bronze`, `silver`, and `gold` prefixes exist in the `warehouse` bucket.

### 3. ClickHouse Tables
Waits for the ClickHouse HTTP interface to be ready, then creates the following tables in `retail_broking` if they do not already exist:

| Table | Description |
|---|---|
| `ohlcv_bars` | 5-minute and 15-minute OHLCV bars, partitioned by `symbol` |
| `vwap_daily` | Daily VWAP per symbol, partitioned by `symbol` |

### 4. Kafka Topic Setup
Verifies that all required Kafka topics exist. If a topic is missing, it is created automatically.

Current topics:
- `market-data`

### 5. Market Data Push
Reads all CSV files from the `data/` directory and pushes each row as a JSON message to the `market-data` Kafka topic. The header row is skipped.

Each message has the shape:
```json
{"Datetime": "...", "Open": ..., "High": ..., "Low": ..., "Close": ..., "Volume": ..., "Symbol": "..."}
```

---

## Verifying the data

### Peek at the first 5 messages
```bash
docker exec -it kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic market-data \
  --from-beginning \
  --max-messages 5
```

### Check the total message count
```bash
docker exec -it kafka kafka-run-class kafka.tools.GetOffsetShell \
  --broker-list localhost:9092 \
  --topic market-data \
  --time -1
```
The number in the last column is the total number of messages in the topic.

---

## ClickHouse — OLAP Store

ClickHouse stores aggregated OHLCV bars and daily VWAP results produced by the Flink job.

| | |
|---|---|
| **HTTP interface** | http://localhost:8123 |
| **Native TCP port** | localhost:19000 |
| **User** | `default` (no password) |
| **Database** | `retail_broking` |

### Table schemas

#### `ohlcv_bars`
```sql
CREATE TABLE ohlcv_bars (
    symbol       LowCardinality(String),
    interval     LowCardinality(String),   -- '5MIN' or '15MIN'
    window_start DateTime,
    window_end   DateTime,
    open         Float64,
    high         Float64,
    low          Float64,
    close        Float64,
    volume       UInt64
) ENGINE = MergeTree()
PARTITION BY symbol
ORDER BY (symbol, interval, window_start);
```

#### `vwap_daily`
```sql
CREATE TABLE vwap_daily (
    symbol       LowCardinality(String),
    date         Date,
    vwap         Float64,
    total_volume UInt64
) ENGINE = MergeTree()
PARTITION BY symbol
ORDER BY (symbol, date);
```

### Query examples

```bash
# Check tables exist
curl -s "http://localhost:8123/?database=retail_broking" \
  --data-binary "SHOW TABLES"

# Count rows in ohlcv_bars
curl -s "http://localhost:8123/?database=retail_broking" \
  --data-binary "SELECT count() FROM ohlcv_bars"

# Latest 5-minute bars for a symbol
curl -s "http://localhost:8123/?database=retail_broking" \
  --data-binary "SELECT * FROM ohlcv_bars WHERE symbol = 'HDFCBANK.NS' AND interval = '5MIN' ORDER BY window_start DESC LIMIT 10"

# Daily VWAP for all symbols
curl -s "http://localhost:8123/?database=retail_broking" \
  --data-binary "SELECT symbol, date, vwap, total_volume FROM vwap_daily ORDER BY symbol, date"
```

### Interactive shell
```bash
docker exec -it clickhouse clickhouse-client --database retail_broking
```

---

## MinIO — Object Storage

MinIO provides S3-compatible object storage for Iceberg tables. It starts automatically with the other services.

| | |
|---|---|
| **Console** | http://localhost:9001 |
| **API endpoint** | http://localhost:9000 |
| **Username** | `minioadmin` |
| **Password** | `minioadmin` |
| **Bucket** | `warehouse` |

Iceberg tables will be written under `s3://warehouse/` (Bronze → `s3://warehouse/bronze/`, Silver → `s3://warehouse/silver/`, Gold → `s3://warehouse/gold/`).

### Verify the bucket exists
```bash
docker exec minio-init mc ls local/
```

---

## Kafka Connect Iceberg Sink — market-data → Bronze

The `kafka-connect-s3` container (Confluent Kafka Connect + Iceberg Kafka Connect runtime 0.6.19 by Tabular/Databricks) reads the `market-data` topic and writes a proper Iceberg table to `s3://warehouse/bronze/market_data/`.

| | |
|---|---|
| **REST API** | http://localhost:8084 |
| **Connector name** | `iceberg-sink-market-data` |
| **Connector class** | `io.tabular.iceberg.connect.IcebergSinkConnector` |
| **Catalog** | Hadoop catalog (metadata stored in MinIO alongside data) |
| **Table** | `bronze.market_data` |
| **Destination** | `s3://warehouse/bronze/market_data/` |
| **Format** | Iceberg (Parquet data files + manifest + snapshot metadata) |

The table is auto-created on first write. Schema is inferred from the JSON value and evolves automatically if new fields appear.

Layout in MinIO after first write:
```
warehouse/bronze/market_data/
  data/
    *.parquet
  metadata/
    v1.metadata.json        ← Iceberg snapshot
    snap-*.avro             ← manifest list
    *.avro                  ← manifest files
```

> **Partitioning**: the table is created unpartitioned initially. `Datetime` arrives as a plain `yyyy-MM-dd HH:mm:ss` string, so Iceberg time transforms (`days()`, `hours()`) cannot be applied directly. To add `identity(Symbol)` partitioning after the first write, run `ALTER TABLE hadoop.bronze.market_data ADD PARTITION FIELD identity(Symbol)` via Spark with the same Hadoop catalog config.

### Check connector status
```bash
curl -s http://localhost:8084/connectors/iceberg-sink-market-data/status | jq .
```

### List Iceberg metadata written to bronze
```bash
docker run --rm --network container:minio --entrypoint /bin/sh minio/mc:latest -c \
  "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1 && mc ls --recursive local/warehouse/bronze/"
```

### Read the table from Spark (to verify Iceberg format)
```python
spark.read.format("iceberg") \
    .load("hadoop.bronze.market_data")  # using HadoopCatalog pointing to s3://warehouse/
```

---

## Running the Flink Job

> **Note:** The pre-built JAR is already available at `flink_processing/target/flink-processing-1.0-SNAPSHOT.jar`. Only rebuild if you have made code changes to the Flink application.

### (Only if code changed) Rebuild the JAR
```bash
cd flink_processing && mvn clean package && cd ../
```

### Submit the job to Flink
```bash
docker cp flink_processing/target/flink-processing-1.0-SNAPSHOT.jar flink-jobmanager:/tmp/
docker exec flink-jobmanager flink run /tmp/flink-processing-1.0-SNAPSHOT.jar
```

The job consumes from `market-data` (earliest offset), computes 5-min and 15-min OHLCV bars plus daily VWAP per symbol, and currently prints results to the TaskManager logs. The ClickHouse sink (writing to `ohlcv_bars` and `vwap_daily`) is the next integration step.

### Monitor the output
```bash
docker logs -f flink-taskmanager
```

---

## Spark Processing — Bronze → Silver → Gold (Iceberg)

The `spark_processing/` module is a Java Spark job that runs the medallion pipeline
after the bronze layer is populated by Flink or Kafka Connect.

| | |
|---|---|
| **Source** | `local.bronze.market_data` (Iceberg) |
| **Silver output** | `local.silver.market_data` |
| **Gold outputs** | `local.gold.ohlcv_5min`, `local.gold.ohlcv_15min`, `local.gold.ohlcv_1hour`, `local.gold.daily_summary` |
| **Catalog** | Hadoop catalog → `s3a://warehouse/` (MinIO) |
| **Format** | Iceberg over Parquet/Snappy |

### Build the JAR

```bash
cd spark_processing && mvn clean package && cd ..
```

The shaded fat JAR is produced at `spark_processing/target/spark-processing-1.0-SNAPSHOT.jar`.

### Step 1 — Bronze → Silver

Reads the raw bronze Iceberg table, applies cleaning (type casting, null drops,
OHLC sanity, deduplication) and writes a partitioned silver Iceberg table.

```bash
spark-submit \
  --master local[*] \
  --class com.retailbroking.BronzeToSilverJob \
  spark_processing/target/spark-processing-1.0-SNAPSHOT.jar
```

**Silver transformations applied:**
- `Datetime` string → `event_time` (TimestampType, UTC)
- Column rename to snake_case; explicit type casts
- Reject nulls in key fields; reject rows where `high < low` or any price ≤ 0
- Deduplicate on `(symbol, event_time)` — idempotent on replay
- Add `event_date` (DateType) and `event_hour` (IntegerType) for partition pruning

**Partition layout:** `s3a://warehouse/silver/market_data/symbol=HDFCBANK.NS/event_date=2024-01-15/`

### Step 2 — Silver → Gold

Reads silver and writes four analytical Iceberg tables to gold.

```bash
spark-submit \
  --master local[*] \
  --class com.retailbroking.SilverToGoldJob \
  spark_processing/target/spark-processing-1.0-SNAPSHOT.jar
```

**Gold tables produced:**

| Table | Description | Partition |
|---|---|---|
| `gold.ohlcv_5min` | 5-minute OHLCV bars | `symbol, event_date` |
| `gold.ohlcv_15min` | 15-minute OHLCV bars | `symbol, event_date` |
| `gold.ohlcv_1hour` | 1-hour OHLCV bars | `symbol, event_date` |
| `gold.daily_summary` | Daily OHLCV + VWAP + daily return % + tick count | `symbol, event_date` |

**OHLC formula** (matches Flink `OHLCVWindowFunction`):
- `open` = opening price of the first tick (min `event_time`) in the window
- `high` = max of all `high` values in the window
- `low` = min of all `low` values in the window
- `close` = closing price of the last tick (max `event_time`) in the window

**VWAP formula** (matches Flink `VWAPWindowFunction`):
```
typical_price = (high + low + close) / 3
vwap          = sum(typical_price × volume) / sum(volume)
```

### Running inside Docker (on the same network as MinIO)

```bash
# Set endpoint to internal Docker DNS name
export MINIO_ENDPOINT=http://minio:9000

spark-submit \
  --master local[*] \
  --class com.retailbroking.BronzeToSilverJob \
  spark_processing/target/spark-processing-1.0-SNAPSHOT.jar
```

### Query gold tables via Spark shell

```python
spark.table("local.gold.daily_summary") \
     .filter("symbol = 'HDFCBANK.NS'") \
     .orderBy("event_date") \
     .show()
```

### Verify files in MinIO

```bash
# Silver
docker run --rm --network container:minio --entrypoint /bin/sh minio/mc:latest -c \
  "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1 && \
   mc ls --recursive local/warehouse/silver/"

# Gold
docker run --rm --network container:minio --entrypoint /bin/sh minio/mc:latest -c \
  "mc alias set local http://localhost:9000 minioadmin minioadmin >/dev/null 2>&1 && \
   mc ls --recursive local/warehouse/gold/"
```

---

## Cube Semantic Layer — Querying Gold via REST API

Cube sits on top of **Trino**, which federates across the Gold Iceberg tables via
the Polaris REST catalog. Cube exposes a REST and GraphQL API so that Conversational
BI and MIS consumers can query business metrics without writing SQL.

### Cube model files

The Cube data model lives in `cube/model/`. Each YAML file maps one Gold table to
a Cube *cube* with typed dimensions and aggregation measures:

| File | Gold Table | Primary use |
|---|---|---|
| `cube/model/ohlcv_bars.yaml` | `gold.ohlcv_5min`, `gold.ohlcv_15min`, `gold.ohlcv_1hour` | OHLCV queries, bar-level analytics |
| `cube/model/daily_summary.yaml` | `gold.daily_summary` | End-of-day P&L, volatility, VWAP |
| `cube/model/trade_summary.yaml` | `gold.trade_summary` | Brokerage revenue, MIS reporting |
| `cube/model/account_positions.yaml` | `gold.account_positions` | Open positions, exposure |
| `cube/model/nav_daily.yaml` | `gold.nav_daily` | Mutual fund NAV history |
| `cube/model/settlement_status.yaml` | `gold.settlement_status` | Compliance, pending settlement |

### Cube connection (Trino → Gold Iceberg)

```yaml
# cube/cube.yml
datasources:
  default:
    type: trino
    host: "${TRINO_HOST}"       # e.g. trino-coordinator
    port: "${TRINO_PORT}"       # default 8080
    catalog: "${TRINO_CATALOG}" # e.g. iceberg (registered in Polaris)
    schema: gold
    user: "${CUBE_USER}"
```

### Query Gold via Cube REST API

The Cube REST endpoint is `POST /cubejs-api/v1/load`.

#### Example 1 — Daily summary for HDFCBANK over the last 7 days

```bash
curl -X POST http://localhost:4000/cubejs-api/v1/load \
  -H "Authorization: $CUBE_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "dimensions":  ["DailySummary.symbol", "DailySummary.event_date"],
      "measures":    ["DailySummary.max_high", "DailySummary.min_low",
                      "DailySummary.total_volume", "DailySummary.avg_vwap",
                      "DailySummary.avg_daily_return_pct"],
      "filters": [
        { "member": "DailySummary.symbol", "operator": "equals", "values": ["HDFCBANK.NS"] }
      ],
      "timeDimensions": [{
        "dimension": "DailySummary.event_date",
        "dateRange": "last 7 days"
      }],
      "order": [["DailySummary.event_date", "asc"]]
    }
  }'
```

Cube generates and sends to Trino:
```sql
SELECT
  symbol,
  event_date,
  MAX(high)              AS max_high,
  MIN(low)               AS min_low,
  SUM(volume)            AS total_volume,
  AVG(vwap)              AS avg_vwap,
  AVG(daily_return_pct)  AS avg_daily_return_pct
FROM gold.daily_summary
WHERE symbol = 'HDFCBANK.NS'
  AND event_date >= CURRENT_DATE - INTERVAL '7' DAY
  AND event_date <  CURRENT_DATE + INTERVAL '1' DAY
GROUP BY symbol, event_date
ORDER BY event_date ASC
```

---

#### Example 2 — Monthly brokerage revenue (Conversational BI query)

Natural language: *"What was the total brokerage collected last month?"*

LLM resolves to this Cube query:
```bash
curl -X POST http://localhost:4000/cubejs-api/v1/load \
  -H "Authorization: $CUBE_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "measures": ["TradeSummary.total_brokerage", "TradeSummary.total_charges",
                   "TradeSummary.total_revenue", "TradeSummary.active_accounts"],
      "timeDimensions": [{
        "dimension": "TradeSummary.event_date",
        "granularity": "month",
        "dateRange": "last month"
      }]
    }
  }'
```

---

#### Example 3 — 5-minute OHLCV bars for a symbol on a specific date

```bash
curl -X POST http://localhost:4000/cubejs-api/v1/load \
  -H "Authorization: $CUBE_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "dimensions": ["OhlcvBars5Min.symbol", "OhlcvBars5Min.window_start",
                     "OhlcvBars5Min.open",   "OhlcvBars5Min.high",
                     "OhlcvBars5Min.low",    "OhlcvBars5Min.close"],
      "measures": ["OhlcvBars5Min.sum_volume"],
      "filters": [
        { "member": "OhlcvBars5Min.symbol", "operator": "equals", "values": ["RELIANCE.NS"] }
      ],
      "timeDimensions": [{
        "dimension": "OhlcvBars5Min.window_start",
        "dateRange": ["2026-06-28", "2026-06-28"]
      }],
      "order": [["OhlcvBars5Min.window_start", "asc"]]
    }
  }'
```

---

#### Example 4 — Pending settlement count (compliance)

```bash
curl -X POST http://localhost:4000/cubejs-api/v1/load \
  -H "Authorization: $CUBE_API_TOKEN" \
  -H "Content-Type: application/json" \
  -d '{
    "query": {
      "dimensions": ["SettlementStatus.settlement_state"],
      "measures":   ["SettlementStatus.total_trades", "SettlementStatus.pending_trades",
                     "SettlementStatus.failed_trades"],
      "timeDimensions": [{
        "dimension": "SettlementStatus.settlement_date",
        "dateRange": "last 7 days"
      }]
    }
  }'
```

---

### DRL → Cube query converter

`cube/drl_to_cube.py` converts a Drools Rule Language (DRL) condition block
into a Cube REST API query and optionally executes it.

```bash
# Install dependency
pip install requests

# Run the converter (dry-run — prints Cube query JSON, does not call API)
python cube/drl_to_cube.py
```

Example DRL rule → Cube query translation:

```python
from cube.drl_to_cube import DrlToCubeQuery

converter = DrlToCubeQuery()

drl = """
    rule "High Brokerage Accounts"
    when
      $t: TradeSummary(
        brokerage > 10000,
        trade_date >= "2026-06-01",
        trade_date <= "2026-06-28"
      )
    then
    end
"""

# Get the Cube query dict (Cube turns this into SQL → Trino → Gold Iceberg)
cube_query = converter.convert(drl)
print(cube_query)

# Or execute directly against a running Cube instance
# rows = converter.execute(drl)
```

Produces:
```json
{
  "measures": [
    "TradeSummary.total_brokerage",
    "TradeSummary.total_fills",
    "TradeSummary.active_accounts"
  ],
  "dimensions": ["TradeSummary.account_id", "TradeSummary.event_date"],
  "filters": [
    { "member": "TradeSummary.total_brokerage", "operator": "gt", "values": ["10000"] }
  ],
  "timeDimensions": [{
    "dimension": "TradeSummary.event_date",
    "dateRange": ["2026-06-01", "2026-06-28"]
  }],
  "limit": 1000
}
```

Cube sends to Trino:
```sql
SELECT account_id, event_date,
       SUM(brokerage) AS total_brokerage,
       SUM(fills)     AS total_fills,
       COUNT(DISTINCT account_id) AS active_accounts
FROM gold.trade_summary
WHERE brokerage > 10000
  AND event_date BETWEEN DATE '2026-06-01' AND DATE '2026-06-28'
GROUP BY account_id, event_date
ORDER BY total_brokerage DESC
```

### Start Cube locally (Docker)

```bash
docker run -d \
  --name cube \
  --network retail-broking_default \
  -p 4000:4000 \
  -e CUBEJS_DB_TYPE=trino \
  -e CUBEJS_DB_HOST=trino-coordinator \
  -e CUBEJS_DB_PORT=8080 \
  -e CUBEJS_DB_CATALOG=iceberg \
  -e CUBEJS_DB_SCHEMA=gold \
  -e CUBEJS_DB_USER=cube_service \
  -e CUBEJS_API_SECRET=dev-secret-change-in-prod \
  -v "$(pwd)/cube/model:/cube/conf/model" \
  -v "$(pwd)/cube/cube.yml:/cube/conf/cube.yml" \
  cubejs/cube:latest
```

Verify Cube is up and the data model loaded:
```bash
curl http://localhost:4000/cubejs-api/v1/meta \
  -H "Authorization: dev-secret-change-in-prod" | jq '.cubes[].name'
```
