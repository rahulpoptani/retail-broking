package com.retailbroking;

import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SparkSession;
import scala.collection.JavaConverters;

import java.util.Arrays;

import static org.apache.spark.sql.functions.*;

/**
 * Silver → Gold
 *
 * Reads the silver market_data Iceberg table and writes four analytical Iceberg
 * tables to the gold layer — all partitioned by (symbol, event_date).
 *
 *   gold.ohlcv_5min     — 5-minute OHLCV bars per symbol
 *   gold.ohlcv_15min    — 15-minute OHLCV bars per symbol
 *   gold.ohlcv_1hour    — 1-hour OHLCV bars per symbol
 *   gold.daily_summary  — Daily OHLCV + VWAP + daily return %
 *
 * ── OHLC formula (matches Flink OHLCVWindowFunction) ──────────────────────────
 *   open   = open price of the FIRST tick in the window  (min event_time)
 *   high   = max(high) across all ticks in the window
 *   low    = min(low)  across all ticks in the window
 *   close  = close price of the LAST tick in the window  (max event_time)
 *   volume = sum(volume)
 *
 * The struct trick: min(struct(event_time, open)) picks the struct whose first
 * field (event_time) is lexicographically smallest, giving us the tick at the
 * window open. max(struct(event_time, close)) gives the window close.
 * This avoids first()/last() inside a groupBy (which have undefined order).
 *
 * ── VWAP formula (matches Flink VWAPWindowFunction) ──────────────────────────
 *   typical_price = (high + low + close) / 3
 *   vwap          = sum(typical_price × volume) / sum(volume)
 *
 * Run locally (Docker Compose stack running, silver table already populated):
 *   spark-submit --class com.retailbroking.SilverToGoldJob \
 *     spark_processing/target/spark-processing-1.0-SNAPSHOT.jar
 */
public class SilverToGoldJob {

    private static final String SILVER_TABLE = "local.silver.market_data";

    public static void main(String[] args) {
        SparkSession spark = SparkSessionFactory.create("Silver → Gold: Aggregations");

        // ── Load silver ───────────────────────────────────────────────────────
        Dataset<Row> silver = spark.table(SILVER_TABLE);
        silver.cache();

        long silverCount = silver.count();
        System.out.printf("[Silver→Gold] Silver rows loaded: %d%n", silverCount);
        if (silverCount == 0) {
            System.out.println("[Silver→Gold] Silver table is empty — run BronzeToSilverJob first.");
            spark.stop();
            return;
        }

        spark.sql("CREATE NAMESPACE IF NOT EXISTS local.gold LOCATION 's3a://warehouse/gold'");

        // ── Intraday OHLCV bars ───────────────────────────────────────────────
        writeOhlcvBars(silver, "5MIN",  "5 minutes",  "local.gold.ohlcv_5min");
        writeOhlcvBars(silver, "15MIN", "15 minutes", "local.gold.ohlcv_15min");
        writeOhlcvBars(silver, "1HOUR", "1 hour",     "local.gold.ohlcv_1hour");

        // ── Daily summary: OHLCV + VWAP + daily return % ─────────────────────
        //
        // Computed in a single pass over silver to avoid shuffling twice.
        // Columns produced:
        //   symbol, event_date,
        //   open, high, low, close    — daily OHLC (same formula as intraday bars)
        //   volume                    — total day volume
        //   vwap                      — typical-price VWAP (rounded to 4 dp)
        //   daily_return_pct          — (close - open) / open × 100 (rounded to 2 dp)
        //   tick_count                — number of raw ticks that day (data quality signal)
        Dataset<Row> dailySummary = silver
                .groupBy(col("symbol"), col("event_date"))
                .agg(
                    // OHLC via struct trick — see class-level comment
                    min(struct(col("event_time"), col("open")))
                            .getField("open").as("open"),
                    max(col("high")).as("high"),
                    min(col("low")).as("low"),
                    max(struct(col("event_time"), col("close")))
                            .getField("close").as("close"),
                    sum(col("volume")).as("volume"),

                    // VWAP = sum((H+L+C)/3 × V) / sum(V)
                    round(
                        sum(
                            col("high").plus(col("low")).plus(col("close"))
                                    .divide(3)
                                    .multiply(col("volume"))
                        ).divide(sum(col("volume"))),
                        4
                    ).as("vwap"),

                    count("*").as("tick_count")
                )
                // Daily return: how much the price moved from open to close (%)
                .withColumn("daily_return_pct",
                    round(
                        col("close").minus(col("open"))
                                .divide(col("open"))
                                .multiply(100),
                        2
                    )
                )
                .select("symbol", "event_date",
                        "open", "high", "low", "close", "volume",
                        "vwap", "daily_return_pct", "tick_count");

        writeGoldTable(dailySummary, "local.gold.daily_summary");
        System.out.printf("[Silver→Gold] daily_summary → local.gold.daily_summary%n");

        System.out.println("[Silver→Gold] Done.");
        System.out.println("  local.gold.ohlcv_5min");
        System.out.println("  local.gold.ohlcv_15min");
        System.out.println("  local.gold.ohlcv_1hour");
        System.out.println("  local.gold.daily_summary");

        spark.stop();
    }

    /**
     * Aggregates silver ticks into fixed-duration OHLCV bars using Spark's
     * time-based window function (tumbling, no slide, no gap).
     *
     * The resulting table has columns:
     *   symbol, interval, window_start, window_end,
     *   open, high, low, close, volume, event_date
     */
    private static void writeOhlcvBars(Dataset<Row> silver,
                                        String label,
                                        String windowDuration,
                                        String table) {
        Dataset<Row> bars = silver
                .groupBy(
                    col("symbol"),
                    // window() creates a struct{start: Timestamp, end: Timestamp}
                    // representing the tumbling time bucket each tick falls into
                    window(col("event_time"), windowDuration).as("w")
                )
                .agg(
                    min(struct(col("event_time"), col("open")))
                            .getField("open").as("open"),
                    max(col("high")).as("high"),
                    min(col("low")).as("low"),
                    max(struct(col("event_time"), col("close")))
                            .getField("close").as("close"),
                    sum(col("volume")).as("volume")
                )
                .select(
                    col("symbol"),
                    lit(label).as("interval"),
                    col("w.start").as("window_start"),
                    col("w.end").as("window_end"),
                    col("open"),
                    col("high"),
                    col("low"),
                    col("close"),
                    col("volume"),
                    to_date(col("w.start")).as("event_date")   // for partitioning
                );

        writeGoldTable(bars, table);
        System.out.printf("[Silver→Gold] %s bars → %s%n", label, table);
    }

    /**
     * Writes a gold DataFrame as an Iceberg table, partitioned by (symbol, event_date).
     * Uses createOrReplace so the job is idempotent across reruns.
     */
    private static void writeGoldTable(Dataset<Row> df, String table) {
        df.writeTo(table)
                .tableProperty("write.format.default", "parquet")
                .tableProperty("write.parquet.compression-codec", "snappy")
                .tableProperty("write.metadata.delete-after-commit.enabled", "true")
                .tableProperty("write.metadata.previous-versions-max", "5")
                .partitionedBy(col("symbol"),
                        JavaConverters.asScalaBuffer(Arrays.asList(col("event_date"))).toSeq())
                .createOrReplace();
    }
}
