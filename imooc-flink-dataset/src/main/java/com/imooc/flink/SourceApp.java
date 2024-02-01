package com.imooc.flink;

import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.configuration.Configuration;

public class SourceApp {

    public static void main(String[] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        Configuration configuration = new Configuration();
        configuration.setBoolean("recursive.file.enumeration", true);

        environment.readTextFile("data/nest")
                .withParameters(configuration)
                .print();
    }
}
