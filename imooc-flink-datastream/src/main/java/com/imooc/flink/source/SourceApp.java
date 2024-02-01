package com.imooc.flink.source;

// import com.imooc.flink.transformation.Access;
import org.apache.flink.api.common.functions.FilterFunction;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.util.NumberSequenceIterator;

import java.util.Properties;

// import org.apache.flink.streaming.api.datastream.DataStreamSource;

public class SourceApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        test01(env);
        env.execute("SourceApp");
    }

    public static void test01(StreamExecutionEnvironment env) {
        env.setParallelism(5);

        DataStreamSource<String> source = env.socketTextStream("localhost", 9527);
        System.out.println("Source....." + source.getParallelism());

        SingleOutputStreamOperator<String> fileStream = source.filter((FilterFunction<String>) value -> !"pk".equals(value)).setParallelism(6);
        System.out.println("filter..." + fileStream.getParallelism());

        fileStream.print();
    }
}
