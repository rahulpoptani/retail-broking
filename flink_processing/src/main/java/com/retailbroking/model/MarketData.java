package com.retailbroking.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

@JsonIgnoreProperties(ignoreUnknown = true)
public class MarketData implements Serializable {
    private static final long serialVersionUID = 1L;
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    @JsonProperty("Datetime") public String datetime;
    @JsonProperty("Open")     public double open;
    @JsonProperty("High")     public double high;
    @JsonProperty("Low")      public double low;
    @JsonProperty("Close")    public double close;
    @JsonProperty("Volume")   public long volume;
    @JsonProperty("Symbol")   public String symbol;

    public long epochMillis() {
        return LocalDateTime.parse(datetime, FORMATTER)
                .toInstant(ZoneOffset.UTC)
                .toEpochMilli();
    }
}
