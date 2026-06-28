package com.retailbroking;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

/**
 * Bronze → Silver
 *
 * Reads the raw market-data Iceberg table from the bronze layer (written by the
 * Kafka Connect Iceberg Sink), applies the standard pre-silver cleaning steps,
 * and writes a curated Iceberg table to the silver layer.
 *
 * Transformations applied (in order):
 *   1. Parse Datetime string → event_time (TimestampType, UTC)
 *      Matches the ZoneOffset.UTC interpretation used by the Flink consumer.
 *   2. Rename PascalCase columns to snake_case; explicit casts to canonical types.
 *   3. Drop rows missing any key field (event_time, symbol, any price column).
 *   4. OHLC sanity: high >= low, all prices > 0, volume >= 0.
 *   5. Deduplicate on (symbol, event_time) — idempotent re-runs are safe.
 *   6. Add event_date (DateType) and event_hour (IntegerType) — used for
 *      partitioning and efficient predicate push-down in downstream queries.
 *
 * Output: local.silver.market_data
 *   Iceberg table, Parquet/Snappy, partitioned by (symbol, event_date)
 *   s3a://warehouse/silver/market_data/
 *
 * Run locally (Docker Compose stack running):
 *   spark-submit --class com.retailbroking.BronzeToSilverJob \
 *     spark_processing/target/spark-processing-1.0-SNAPSHOT.jar
 */
public class BronzeToSilverJob {

    static final String BRONZE_TABLE = "local.bronze.market_data";
    static final String SILVER_TABLE = "local.silver.market_data";

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("Bronze → Silver: Market Data");

        // ── 1. Read bronze Iceberg table ──────────────────────────────────────
        //
        // Schema from Iceberg Kafka Connect Sink (auto-created from JSON):
        //   Datetime STRING, Open DOUBLE, High DOUBLE, Low DOUBLE,
        //   Close DOUBLE, Volume LONG, Symbol STRING
        Dataset<Row> bronze = spark.table(BRONZE_TABLE);

        System.out.println("[Bronze→Silver] Bronze schema:");
        bronze.printSchema();

        long bronzeCount = bronze.count();
        if (bronzeCount == 0) {
            System.out.println("[Bronze→Silver] Bronze table is empty — nothing to process.");
            spark.stop();
            return;
        }
        System.out.printf("[Bronze→Silver] Bronze row count: %d%n", bronzeCount);

        // ── 2. Clean & transform ──────────────────────────────────────────────
        // Use select+alias to rename PascalCase → snake_case and cast in one step.
        // Avoids the Spark case-insensitive drop bug where drop("Open") also
        // removes the newly created "open" column.
        Dataset<Row> silver = bronze
                .select(
                        to_timestamp(col("Datetime"), "yyyy-MM-dd HH:mm:ss").alias("event_time"),
                        col("Symbol").alias("symbol"),
                        col("Open").cast("double").alias("open"),
                        col("High").cast("double").alias("high"),
                        col("Low").cast("double").alias("low"),
                        col("Close").cast("double").alias("close"),
                        col("Volume").cast("long").alias("volume")
                )

                // ── Null safety ───────────────────────────────────────────────
                .filter(col("event_time").isNotNull()
                        .and(col("symbol").isNotNull())
                        .and(col("open").isNotNull())
                        .and(col("high").isNotNull())
                        .and(col("low").isNotNull())
                        .and(col("close").isNotNull()))

                // ── OHLC sanity checks ────────────────────────────────────────
                .filter(col("high").geq(col("low")))   // violated by corrupt ticks
                .filter(col("open").gt(0))
                .filter(col("high").gt(0))
                .filter(col("low").gt(0))
                .filter(col("close").gt(0))
                .filter(col("volume").geq(0))          // negative volume is nonsensical

                // ── Deduplication on natural key ──────────────────────────────
                // Safe to run multiple times: the sink may replay on restart
                .dropDuplicates("symbol", "event_time")

                // ── Partition / filter helper columns ─────────────────────────
                .withColumn("event_date", to_date(col("event_time")))
                .withColumn("event_hour", hour(col("event_time")))

                // Canonical output column order
                .select("symbol", "event_time", "event_date", "event_hour", "open", "high", "low", "close", "volume");

        silver.cache();
        long silverCount = silver.count();
        System.out.printf("[Bronze→Silver] Rows after cleaning: %d  (dropped: %d)%n",
                silverCount, bronzeCount - silverCount);
        System.out.println("[Bronze→Silver] Silver schema:");
        silver.printSchema();

        // ── 3. Write silver Iceberg table ─────────────────────────────────────
        //
        // Partition by symbol + event_date so downstream Spark / Trino queries
        // can push predicates and avoid full-table scans.
        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.silver LOCATION 's3a://warehouse/silver'");

        silver.writeTo(SILVER_TABLE)
                .tableProperty("write.format.default", "parquet")
                .tableProperty("write.parquet.compression-codec", "snappy")
                // Keep only the last 5 metadata snapshots to avoid accumulation
                .tableProperty("write.metadata.delete-after-commit.enabled", "true")
                .tableProperty("write.metadata.previous-versions-max", "5")
                .partitionedBy(col("symbol"),
                        JavaConverters.asScalaBuffer(Arrays.asList(col("event_date"))).toSeq())
                .createOrReplace();

        System.out.printf("[Bronze→Silver] %d rows written to %s%n",
                silverCount, SILVER_TABLE);
        spark.stop();
    }
}
