package com.imooc.flink.basic;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.tuple.Tuple2;

import java.util.Arrays;

public class BatchWCApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> source = environment.readTextFile("data/wc.data");

        source.flatMap((FlatMapFunction<String, String>)(value, out) -> {
            String[] words = value.split(",");
            Arrays.stream(words).map(word -> word.toLowerCase().trim()).forEach(out::collect);
        }).map((MapFunction<String, Tuple2<String, Integer>>) value -> new Tuple2<>(value, 1))
                .groupBy(0)
                .sum(1)
                .print();
    }
}
