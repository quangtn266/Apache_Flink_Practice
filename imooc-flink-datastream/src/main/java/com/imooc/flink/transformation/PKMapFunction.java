package com.imooc.flink.transformation;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.configuration.Configuration;

public class PKMapFunction extends RichMapFunction<String, Access> {

    @Override
    public void open(Configuration parameters) throws Exception {
        super.open(parameters);
        System.out.println("-----open------");
    }

    @Override
    public void close() throws Exception {
        super.close();
    }

    @Override
    public RuntimeContext getRuntimeContext() { return super.getRuntimeContext();}

    @Override
    public Access map(String value) throws Exception {
        System.out.println("------map-----");

        String[] splits = value.split(",");
        Long time = Long.parseLong(splits[0].trim());
        String domain = splits[1].trim();
        Double traffic = Double.parseDouble(splits[2].trim());
        return new Access(time, domain, traffic);
    }
}
