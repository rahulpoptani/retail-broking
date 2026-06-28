package com.retailbroking.model;

import java.io.Serializable;

public class VWAPResult implements Serializable {
    private static final long serialVersionUID = 1L;

    public String symbol;
    public String date;
    public double vwap;
    public long totalVolume;

    @Override
    public String toString() {
        return String.format("[VWAP] %s | %s | VWAP:%.4f | TotalVol:%d",
                symbol, date, vwap, totalVolume);
    }
}
