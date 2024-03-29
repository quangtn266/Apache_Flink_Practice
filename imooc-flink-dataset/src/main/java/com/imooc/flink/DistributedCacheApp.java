package com.imooc.flink;

import org.apache.commons.io.FileUtils;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.configuration.Configuration;

import java.io.File;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;

public class DistributedCacheApp {
    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        environment.registerCachedFile("data/wc.txt", "pk-wc-dc");

        DataSource<String> data = environment.fromElements("hadoop", "spark", "flink", "pyspark");

        data.map(new RichMapFunction<String, String>() {

            List<String> list = new ArrayList<>();

            @Override
            public void open(Configuration parameters) throws Exception {
                File file = getRuntimeContext().getDistributedCache().getFile("pk-wc-dc");
                List<String> lines = FileUtils.readLines(file, Charset.defaultCharset());
                for(String line: lines) {
                    list.add(line);
                    System.out.println("line -->" + line);
                }
            }

            @Override
            public String map(String value) throws Exception {
                return value;
            }
        }).print();
    }
}