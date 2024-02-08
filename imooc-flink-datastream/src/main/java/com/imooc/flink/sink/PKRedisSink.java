package com.imooc.flink.sink;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;

public class PKRedisSink implements RedisMapper<Tuple2<String, Double>> {

    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.HSET, "pk-traffic");
    }

    @Override
    public String getKeyFromData(Tuple2<String, Double> data) {
        return data.f0;
    }

    @Override
    public String getValueFromData(Tuple2<String, Double> data) {
        return data.f1 + "";
    }
}
