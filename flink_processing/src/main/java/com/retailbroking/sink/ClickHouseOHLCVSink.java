package com.retailbroking.sink;

import com.retailbroking.model.OHLCVBar;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class ClickHouseOHLCVSink extends RichSinkFunction<OHLCVBar> {

    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/retail_broking";
    private static final String INSERT_SQL =
            "INSERT INTO ohlcv_bars (symbol, interval, window_start, window_end, open, high, low, close, volume) " +
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)";

    private transient Connection connection;
    private transient PreparedStatement statement;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
        connection = DriverManager.getConnection(CLICKHOUSE_URL);
        statement = connection.prepareStatement(INSERT_SQL);
    }

    @Override
    public void invoke(OHLCVBar bar, Context context) throws Exception {
        statement.setString(1, bar.symbol);
        statement.setString(2, bar.interval);
        statement.setString(3, bar.windowStart);
        statement.setString(4, bar.windowEnd);
        statement.setDouble(5, bar.open);
        statement.setDouble(6, bar.high);
        statement.setDouble(7, bar.low);
        statement.setDouble(8, bar.close);
        statement.setLong(9, bar.volume);
        statement.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        if (statement != null) statement.close();
        if (connection != null) connection.close();
    }
}
