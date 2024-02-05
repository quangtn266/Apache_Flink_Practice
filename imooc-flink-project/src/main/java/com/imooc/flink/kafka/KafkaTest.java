package com.imooc.flink.kafka;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.api.java.tuple.Tuple2;

public class KafkaTest {
    public static void main(String[] args) throws Exception {
        DataStream<Tuple2<String, String>> source = FlinkUtils.createKafkaStreamV3(args, PKKafkaDeserializationSchema.class);
        source.print();
        FlinkUtils.env.execute();
    }
}
