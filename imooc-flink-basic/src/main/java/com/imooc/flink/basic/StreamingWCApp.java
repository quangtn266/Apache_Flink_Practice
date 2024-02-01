package com.imooc.flink.basic;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Arrays;

public class StreamingWCApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

        source.flatMap((FlatMapFunction<String, String>)(value, out) -> {
            String[] words = value.split(",");
            Arrays.stream(words).map(word -> word.toLowerCase().trim()).forEachOrdered(out::collect);
        }).filter((FilterFunction<String>) StringUtils::isNotEmpty)
                .map((MapFunction<String, Tuple2<String, Integer>>)value -> new Tuple2<>(value, 1))
                .keyBy((KeySelector<Tuple2<String, Integer>, String>)value -> value.f0).sum(1).print();
        env.execute("StreamingWCApp");
    }
}
