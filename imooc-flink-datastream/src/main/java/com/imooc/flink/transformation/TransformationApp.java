package com.imooc.flink.transformation;

import com.imooc.flink.source.AccessSource;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.common.functions.ReduceFunction;
import org.apache.flink.api.java.functions.KeySelector;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.datastream.*;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.co.CoFlatMapFunction;
import org.apache.flink.streaming.api.functions.co.CoMapFunction;
import org.apache.flink.util.Collector;

import java.util.ArrayList;

public class TransformationApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        coFlatMap(env);
        env.execute("SourceApp");
    }

    public static void coFlatMap(StreamExecutionEnvironment env) {
        DataStreamSource<String> stream1 = env.fromElements("a b c ", "d e f");
        DataStreamSource<String> stream2 = env.fromElements("1 2 3 ", "4 5 6");

        stream1.connect(stream2).flatMap(new CoFlatMapFunction<String, String, String>() {

            @Override
            public void flatMap1(String value, Collector<String> out) throws Exception {
                String[] splits = value.split(" ");
                for(String split: splits) {
                    out.collect(split);
                }
            }

            @Override
            public void flatMap2(String value, Collector<String> out) throws Exception {
                String[] splits = value.split(",");
                for (String split: splits) {
                    out.collect(split);
                }
            }
        }).print();
    }

    public static void coMap(StreamExecutionEnvironment env) {
        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9527);
        SingleOutputStreamOperator<Integer> stream2 = env.socketTextStream("localhost", 9528)
                .map(new MapFunction<String, Integer>() {

                    @Override
                    public Integer map(String value) throws Exception {
                        return Integer.parseInt(value);
                    }
                });

        stream1.connect(stream2).map(new CoMapFunction<String, Integer, String>() {

            @Override
            public String map1(String value) throws Exception {
                return value.toUpperCase();
            }

            @Override
            public String map2(Integer value) throws Exception {
                return value * 10 + "";
            }
        }).print();
    }

    public static void connect(StreamExecutionEnvironment env) {

        DataStreamSource<Access> stream1 = env.addSource(new AccessSource());
        DataStreamSource<Access> stream2 = env.addSource(new AccessSource());

        SingleOutputStreamOperator<Tuple2<String, Access>> stream2new = stream2.map(new MapFunction<Access, Tuple2<String, Access>>() {

            @Override
            public Tuple2<String, Access> map(Access value) throws Exception {
                return Tuple2.of("pk", value);
            }
        });

        stream1.connect(stream2new).map(new CoMapFunction<Access, Tuple2<String, Access>, String>() {

            @Override
            public String map1(Access value) throws Exception {
                return value.toString();
            }

            @Override
            public String map2(Tuple2<String, Access> value) throws Exception {
                return value.f0 + "-->" + value.f1.toString();
            }
        }).print();
    }

    public static void union(StreamExecutionEnvironment env) {

        DataStreamSource<String> stream1 = env.socketTextStream("localhost", 9527);
        DataStreamSource<String> stream2 = env.socketTextStream("localhost", 9528);

        DataStream<String> union = stream1.union(stream2);
        stream1.union(stream1).print();
    }

    public static void richMap(StreamExecutionEnvironment env) {
        env.setParallelism(3);
        DataStreamSource<String> source = env.readTextFile("data/access.log");

        SingleOutputStreamOperator<Access> mapStream = source.map(new PKMapFunction());
        mapStream.print();
    }

    public static void reduct(StreamExecutionEnvironment env) {

        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);

        source.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] splits = value.split(",");
                for (String word: splits) {
                    out.collect(word);
                }
            }
        }).map(new MapFunction<String, Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> map(String value) throws Exception {
                return Tuple2.of(value, 1);
            }
        }).keyBy(x -> x.f0).reduce(new ReduceFunction<Tuple2<String, Integer>>() {

            @Override
            public Tuple2<String, Integer> reduce(Tuple2<String, Integer> value1, Tuple2<String, Integer> value2) throws Exception {
                return Tuple2.of(value1.f0, value1.f1 + value2.f1);
            }
        }).print();
    }

    public static void keyBy(StreamExecutionEnvironment env) {

        DataStreamSource<String> source = env.readTextFile("data/access.log");

        SingleOutputStreamOperator<Access> mapStream = source.map(new MapFunction<String, Access>() {

            @Override
            public Access map(String value) throws Exception {
                String[] splits =value.split(",");
                Long time = Long.parseLong(splits[0].trim());
                String domain = splits[1].trim();
                Double traffic = Double.parseDouble(splits[2].trim());

                return new Access(time, domain, traffic);
            }
        });

        KeyedStream<Access, String> keyedStream = mapStream.keyBy(x -> x.getDomain());
        keyedStream.sum("traffic").print();
    }

    public static void flatMap(StreamExecutionEnvironment env) {

        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);
        source.flatMap(new FlatMapFunction<String, String>() {

            @Override
            public void flatMap(String value, Collector<String> out) throws Exception {
                String[] splits = value.split(",");
                for(String split: splits) {
                    out.collect(split);
                }
            }
        }).filter(new FilterFunction<String>() {
            @Override
            public boolean filter(String value) throws Exception {
                return !"pk".equals(value);
            }
        }).print();
    }

    public static void filter(StreamExecutionEnvironment env) {

        DataStreamSource<String> source  =env.readTextFile("data/access.log");

        SingleOutputStreamOperator<Access> mapStream = source.map(new MapFunction<String, Access>() {

            @Override
            public Access map(String value) throws Exception {
                String[] splits = value.split(",");
                Long time = Long.parseLong(splits[0].trim());
                String domain = splits[1].trim();
                Double traffic = Double.parseDouble(splits[2].trim());

                return new Access(time, domain, traffic);
            }
        });

        SingleOutputStreamOperator<Access> filterStream = mapStream.filter(new FilterFunction<Access>() {
            @Override
            public boolean filter(Access value) throws Exception {
                return value.getTraffic() > 4000;
            }
        });

        filterStream.print();
    }

    public static void map(StreamExecutionEnvironment env) {

        ArrayList<Integer> list = new ArrayList<>();
        list.add(1);
        list.add(2);
        list.add(3);
        DataStreamSource<Integer> source = env.fromCollection(list);

        source.map(new MapFunction<Integer, Integer>() {

            @Override
            public Integer map(Integer value) throws Exception {
                return value * 2;
            }
        }).print();
    }
}
