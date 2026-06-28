package com.retailbroking.model;

import java.io.Serializable;

public class OHLCVBar implements Serializable {
    private static final long serialVersionUID = 1L;

    public String symbol;
    public String interval;
    public String windowStart;
    public String windowEnd;
    public double open;
    public double high;
    public double low;
    public double close;
    public long volume;

    @Override
    public String toString() {
        return String.format("[%s] %s | %s -> %s | O:%.2f H:%.2f L:%.2f C:%.2f V:%d",
                interval, symbol, windowStart, windowEnd, open, high, low, close, volume);
    }
}
