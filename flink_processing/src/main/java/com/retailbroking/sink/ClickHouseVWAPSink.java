package com.retailbroking.sink;

import com.retailbroking.model.VWAPResult;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

public class ClickHouseVWAPSink extends RichSinkFunction<VWAPResult> {

    private static final String CLICKHOUSE_URL = "jdbc:clickhouse://clickhouse:8123/retail_broking";
    private static final String INSERT_SQL =
            "INSERT INTO vwap_daily (symbol, date, vwap, total_volume) VALUES (?, ?, ?, ?)";

    private transient Connection connection;
    private transient PreparedStatement statement;

    @Override
    public void open(Configuration parameters) throws Exception {
        Class.forName("com.clickhouse.jdbc.ClickHouseDriver");
        connection = DriverManager.getConnection(CLICKHOUSE_URL);
        statement = connection.prepareStatement(INSERT_SQL);
    }

    @Override
    public void invoke(VWAPResult result, Context context) throws Exception {
        statement.setString(1, result.symbol);
        statement.setString(2, result.date);
        statement.setDouble(3, result.vwap);
        statement.setLong(4, result.totalVolume);
        statement.executeUpdate();
    }

    @Override
    public void close() throws Exception {
        if (statement != null) statement.close();
        if (connection != null) connection.close();
    }
}
