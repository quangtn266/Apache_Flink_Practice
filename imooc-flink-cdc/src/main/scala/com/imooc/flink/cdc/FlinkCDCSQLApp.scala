package com.imooc.flink.cdc

import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.Table
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.types.Row
import org.apache.flink.api.scala._

object FlinkCDCSQLApp {

  def main(args: Array[String]): Unit = {
    val environment: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnvironment: StreamTableEnvironment = StreamTableEnvironment.create(environment)
    tableEnvironment.executeSql(
      """
        |create table user_address(
        |is STRING primary key,
        |name STRING,
        |address STRING
        |) with (
        |'connector' = 'mysql-cdc',
        |'hostname' = 'localhost',
        |'port' = '3306',
        |'username' = 'root',
        |'password' = 'pk123456',
        |'database-name' = 'pk_cdc',
        |'table-name' = 'user'
        |)
        |""".stripMargin
    )

    val table: Table = tableEnvironment.sqlQuery("select * from user_address")
    tableEnvironment.toRetractStream[Row](table).print()

    environment.execute()
  }
}
