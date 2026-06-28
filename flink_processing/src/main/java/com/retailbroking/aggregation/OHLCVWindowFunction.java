package com.retailbroking.aggregation;

import com.retailbroking.model.MarketData;
import com.retailbroking.model.OHLCVBar;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;

import java.time.Instant;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

public class OHLCVWindowFunction extends ProcessWindowFunction<MarketData, OHLCVBar, String, TimeWindow> {

    private static final DateTimeFormatter FMT =
            DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").withZone(ZoneOffset.UTC);

    private final String interval;

    public OHLCVWindowFunction(String interval) {
        this.interval = interval;
    }

    @Override
    public void process(String symbol, Context ctx, Iterable<MarketData> elements, Collector<OHLCVBar> out) {
        OHLCVBar bar = new OHLCVBar();
        bar.symbol   = symbol;
        bar.interval = interval;
        bar.windowStart = FMT.format(Instant.ofEpochMilli(ctx.window().getStart()));
        bar.windowEnd   = FMT.format(Instant.ofEpochMilli(ctx.window().getEnd()));

        boolean first = true;
        for (MarketData md : elements) {
            if (first) {
                bar.open = md.open;
                bar.high = md.high;
                bar.low  = md.low;
                first    = false;
            } else {
                bar.high = Math.max(bar.high, md.high);
                bar.low  = Math.min(bar.low,  md.low);
            }
            bar.close   = md.close;
            bar.volume += md.volume;
        }

        out.collect(bar);
    }
}
