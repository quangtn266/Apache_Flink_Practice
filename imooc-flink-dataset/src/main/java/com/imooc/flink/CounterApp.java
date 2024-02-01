package com.imooc.flink;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.accumulators.LongCounter;
import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.java.ExecutionEnvironment;
import org.apache.flink.api.java.operators.DataSource;
import org.apache.flink.api.java.operators.MapOperator;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileSystem;


public class CounterApp {

    public static void main(String [] args) throws Exception {
        ExecutionEnvironment environment = ExecutionEnvironment.getExecutionEnvironment();

        DataSource<String> data = environment.fromElements("hadoop", "spark", "flink", "pyspark");

        MapOperator<String, String> info = data.map(new RichMapFunction<String, String>(){
          LongCounter counter = new LongCounter();

          @Override
          public void open(Configuration parameters) throws Exception {
              getRuntimeContext().addAccumulator("ele-cnts", counter);
          }

          @Override
          public String map (String value) throws Exception {
              counter.add(1);
              return value;
          }
        });

        info.writeAsText("out", FileSystem.WriteMode.OVERWRITE).setParallelism(2);
        JobExecutionResult result = environment.execute("CounterApp");

        Object acc = result.getAccumulatorResult("ele-cnts");
        System.out.println(acc);
    }
}
