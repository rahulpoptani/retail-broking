package com.retailbroking.aggregation;

import com.retailbroking.model.MarketData;
import com.retailbroking.model.VWAPResult;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class VWAPWindowFunction extends ProcessWindowFunction<MarketData, VWAPResult, String, TimeWindow> {

    private static final DateTimeFormatter DATE_FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd").withZone(ZoneOffset.UTC);

    @Override
    public void process(String symbol, Context ctx, Iterable<MarketData> elements, Collector<VWAPResult> out) {
        double sumTPV   = 0.0;
        long sumVolume  = 0;

        for (MarketData md : elements) {
            double typicalPrice = (md.high + md.low + md.close) / 3.0;
            sumTPV    += typicalPrice * md.volume;
            sumVolume += md.volume;
        }

        VWAPResult result   = new VWAPResult();
        result.symbol       = symbol;
        result.date         = DATE_FMT.format(Instant.ofEpochMilli(ctx.window().getStart()));
        result.vwap         = sumVolume > 0 ? sumTPV / sumVolume : 0.0;
        result.totalVolume  = sumVolume;

        out.collect(result);
    }
}
