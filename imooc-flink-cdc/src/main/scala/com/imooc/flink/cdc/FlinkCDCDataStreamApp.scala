package com.imooc.flink.cdc

import com.alibaba.ververica.cdc.connectors.mysql.MySQLSource
import com.alibaba.ververica.cdc.connectors.mysql.table.StartupOptions
import com.alibaba.ververica.cdc.debezium.{DebeziumSourceFunction, StringDebeziumDeserializationSchema}
import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.scala._
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig

object FlinkCDCDataStreamApp {
  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    environment.setParallelism(1)
    environment.enableCheckpointing(5000L)
    environment.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    environment.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    environment.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, 5000L))

    environment.setStateBackend(new FsStateBackend("hdfs://localhost:8020/flink-ck/cdc"))

    val source: DebeziumSourceFunction[String] = MySQLSource.builder()
      .hostname("localhost")
      .port(3306)
      .username("root")
      .password("pk123456")
      .databaseList("pk_cdc")
      .tableList("pk_cdc.user")
      .startupOptions(StartupOptions.initial())
      .deserializer(new StringDebeziumDeserializationSchema)
      .build()

  environment.addSource(source).print()

  environment.execute()
  }
}
