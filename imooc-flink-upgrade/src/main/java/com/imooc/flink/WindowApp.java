package com.imooc.flink;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.Tumble;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

public class WindowApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);

        StreamTableEnvironment tableEnv = StreamTableEnvironment.create(env);

        SingleOutputStreamOperator<Tuple4<Long, String, String, Double>> input = env.fromElements(
                "1000,pk,spark,75",
                "2000,pk,flink,65",
                "2000,zs,kuangquanshui,3",
                "3000,pk,cdh,65",
                "9999,zs,xuebi,3",
                "19999,pk,Hive,45"
        ).map(new MapFunction<String, Tuple4<Long, String, String, Double>>() {

            @Override
            public Tuple4<Long, String, String, Double> map(String value) throws Exception {
                String[] splits = value.split(",");
                Long time = Long.parseLong(splits[0]);
                String user = splits[1];
                String book = splits[2];
                Double money = Double.parseDouble(splits[3]);
                return Tuple4.of(time, user, book, money);
            }
        }).assignTimestampsAndWatermarks(

                new BoundedOutOfOrdernessTimestampExtractor<Tuple4<Long, String, String, Double>>(Time.seconds(0)) {

                    @Override
                    public long extractTimestamp(Tuple4<Long, String, String, Double> element) {
                        return element.f0;
                    }
                }
        );


        tableEnv.createTemporaryView("access", input, "time,user_id,book,money,rowtime.rowtime");

        Table resultTable = tableEnv.from("access")
                .window(Tumble.over("10.seconds").on("rowtime").as("win"))
                .groupBy("user_id, win")
                .select("user_id, win.start, win.end, money.sum as total");

        tableEnv.toRetractStream(resultTable, Row.class).filter(x -> x.f0).print();

        env.execute("WindowApp");
    }
}
