package com.imooc.flink;

import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.util.Collector;

import java.util.Arrays;
import java.util.List;

public class StreamWCApp {

    public static void main(String[] args) throws Exception {

        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        List<String> data = Arrays.asList("pk, pk, pk", "flink, flink");

        environment.fromCollection(data)
                .flatMap((String line, Collector<String> outer) -> Arrays.stream(line.split(","))
                        .forEach(outer::collect))
                .returns(Types.STRING)
                .map(x -> Tuple2.of(x, 1))
                .returns(Types.TUPLE(Types.STRING, Types.INT))
                .keyBy((KeySelector<Tuple2<String, Integer>, String>) input -> input.f0)
                .sum(1)
                .print();

        environment.execute();
    }
}
