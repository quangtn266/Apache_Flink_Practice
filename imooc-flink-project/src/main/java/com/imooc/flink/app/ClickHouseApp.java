package com.imooc.flink.app;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connector.jdbc.JdbcConnectionOptions;
import org.apache.flink.connector.jdbc.JdbcExecutionOptions;
import org.apache.flink.connector.jdbc.JdbcSink;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;



public class ClickHouseApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        // col1, col2, col3
        env.socketTextStream("localhost", 9527)
                .map(new MapFunction<String, Tuple3<String, String, String>>() {

                    @Override
                    public Tuple3<String, String, String> map(String value) throws Exception {
                        String[] splits = value.split(",");
                        return new Tuple3.of(splits[0].trim, splits[1].trim(), splits[2].trim());
                    }
                }).addSink(JdbcSink.sink(
                        "insert into ch_test values(?,?,?)",
                        (pstmt, x) -> {
                            pstmt.setString(1, x.f0);
                            pstmt.setString(2, x.f1);
                            pstmt.setString(3, x.f2);
                        },

                JdbcExecutionOptions.builder().withBatchSize(3).withBatchIntervalMs(4000).build(),
                new JdbcConnectionOptions().JdbcConnectionOptionsBuilder()
                        .withUrl("jdbc:clickhouse://ruozedata001:8123/pk")
                        .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
                        .build()
                ));

        env.execute();
    }
}
