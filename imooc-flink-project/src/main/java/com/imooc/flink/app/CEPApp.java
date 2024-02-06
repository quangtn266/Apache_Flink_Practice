package com.imooc.flink.app;

import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.cep.CEP;
import org.apache.flink.cep.PatternSelectFunction;
import org.apache.flink.cep.PatternStream;
import org.apache.flink.cep.pattern.Pattern;
import org.apache.flink.cep.pattern.conditions.SimpleCondition;
import org.apache.flink.streaming.api.datastream.KeyedStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor;
import org.apache.flink.streaming.api.windowing.time.Time;

import java.util.List;
import java.util.Map;

// import com.imooc.flink.domain.Access;

public class CEPApp {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment environment = StreamExecutionEnvironment.getExecutionEnvironment();

        environment.setParallelism(1);

        KeyedStream<Access, String> stream = environment.fromElements(
                "001,78.89.90.2,success,1622689918",
                "002,110.111.112.113,failure,1622689952",
                "002,110.111.112.113,failure,1622689953",
                "002,110.111.112.113,failure,1622689954",
                "002,193.114.45.13,success,1622689959",
                "002,137.49.24.26,failure,1622689958"
        ).map(new MapFunction<String, Access>() {

            @Override
            public Access map(String value) throws Exception {
                String[] splits = value.split(",");

                Access access = new Access();
                access.userId = splits[0];
                access.ip = splits[1];
                access.result = splits[2];
                access.time = Long.parseLong(splits[3]);

                return access;
            }
        }).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor<Access>(Time.seconds(5)) {

            @Override
            public long extractTimestamp(Access element) {
                return element.time * 1000;
            }
        }).keyBy(x -> x.userId);

        // setting rule
        Pattern<Access, Access> pattern = Pattern.<Access>begin("start")
                .where(new SimpleCondition<Access>(){

                    @Override
                    public boolean filter(Access value) throws Exception {
                        return value.result.equals("failure");
                    }
                }).next("next")
                .where(new SimpleCondition<Access>(){

                    @Override
                    public boolean filter(Access value) throws Exception {
                        return value.result.equals("failure");
                    }
                }).within(Time.seconds(3));

        // Apply rule to data pipeline
        PatternStream<Access> patternStream = CEP.pattern(stream, pattern);

        // Extract data from the rule
        patternStream.select(new PatternSelectFunction<Access, AccessMsg>() {

            @Override
            public AccessMsg select(Map<String, List<Access>> pattern) throws Exception {

                Access first = pattern.get("start").get(0);
                Access last =pattern.get("next").get(0);

                AccessMsg  msg = new AccessMsg();
                msg.userId = first.userId;
                msg.first = first.time;
                msg.second = last.time;
                msg.msg = "Error access!!!!";

                return msg;
            }
        }).print();

        environment.execute("CEPApp");
    }
}

class AccessMsg {

    public String userId;
    public Long first;
    public Long second;
    public String msg;

    @Override
    public String toString() {
        return "AccessMsg{" +
                "userId='" +userId + '\'' +
                ", first=" + first +
                ", second=" + second +
                ", msg='" + msg + '\'' +
                '}';
    }
}

class Access {
    public String userId;
    public String ip;
    public String result;
    public Long time;
}