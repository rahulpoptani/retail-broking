package com.retailbroking;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.retailbroking.aggregation.OHLCVWindowFunction;
import com.retailbroking.aggregation.VWAPWindowFunction;
import com.retailbroking.model.MarketData;
import com.retailbroking.sink.ClickHouseOHLCVSink;
import com.retailbroking.sink.ClickHouseVWAPSink;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

public class MarketDataConsumer {

    private static final String KAFKA_BROKERS = "kafka:29092";
    private static final String TOPIC         = "market-data";
    private static final String GROUP_ID      = "flink-market-data-consumer";

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(KAFKA_BROKERS)
                .setTopics(TOPIC)
                .setGroupId(GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<MarketData> marketData = env
                .fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka: " + TOPIC)
                .map(new JsonToMarketData())
                .assignTimestampsAndWatermarks(
                        WatermarkStrategy.<MarketData>forMonotonousTimestamps()
                                .withTimestampAssigner((event, ts) -> event.epochMillis())
                );

        // 5-minute OHLCV bars per symbol → ohlcv_bars
        marketData
                .keyBy(md -> md.symbol)
                .window(TumblingEventTimeWindows.of(Time.minutes(5)))
                .process(new OHLCVWindowFunction("5MIN"))
                .addSink(new ClickHouseOHLCVSink())
                .name("ClickHouse: ohlcv_bars (5MIN)");

        // 15-minute OHLCV bars per symbol → ohlcv_bars
        marketData
                .keyBy(md -> md.symbol)
                .window(TumblingEventTimeWindows.of(Time.minutes(15)))
                .process(new OHLCVWindowFunction("15MIN"))
                .addSink(new ClickHouseOHLCVSink())
                .name("ClickHouse: ohlcv_bars (15MIN)");

        // Daily VWAP per symbol → vwap_daily
        marketData
                .keyBy(md -> md.symbol)
                .window(TumblingEventTimeWindows.of(Time.days(1)))
                .process(new VWAPWindowFunction())
                .addSink(new ClickHouseVWAPSink())
                .name("ClickHouse: vwap_daily");

        env.execute("Market Data Aggregation");
    }

    // ObjectMapper is not serializable; lazy-init via transient field avoids closure issues
    private static class JsonToMarketData implements MapFunction<String, MarketData> {
        private transient ObjectMapper mapper;

        @Override
        public MarketData map(String json) throws Exception {
            if (mapper == null) mapper = new ObjectMapper();
            return mapper.readValue(json, MarketData.class);
        }
    }
}
