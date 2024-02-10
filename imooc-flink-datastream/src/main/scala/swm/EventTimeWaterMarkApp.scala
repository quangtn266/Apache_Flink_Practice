package swm

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.OutputTag

import java.time.Duration

object EventTimeWaterMarkApp {

  @throws[Exception]
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    execute(env)
    env.execute("EventTimeWaterMarkApp")
  }

  def execute(env: StreamExecutionEnvironment): Unit = {

    val outputTag = new OutputTag[(String, Integer)]("late-data") {}

    val watermarkStrategy: WatermarkStrategy[String] = WatermarkStrategy
      .forBoundedOutOfOrderness[String](Duration.ofSeconds(0))
      .withTimestampAssigner(new SerializableTimestampAssigner[String] {
        override def extractTimestamp(element: String, recordTimestamp: Long): Long = element.split(",")(0).asInstanceOf[Long]
      })

    val window = env.socketTextStream("localhost", 9854).filter(_.nonEmpty)
      .assignTimestampsAndWatermarks(watermarkStrategy).map(value => {
        val splits = value.split(",")
        (splits(1).trim, Integer.getInteger(splits(2).trim))
      }).returns(Types.TUPLE[(String, Integer)]).keyBy(0).window(TumblingEventTimeWindows.of(Time.seconds(5)))
      .sideOutputLateData(outputTag).reduce(v1, v2) => {
        println("----reduce invoked -------" + v1._1 + "===>" + (v1._1 + v2._1))
      (v1._1, v1._2 + v2._2)
    })
    window.print.setParallelism(1)
    val sideOutput = window.getSideOutput(outputTag)
    sideOutput.printToErr.setParallelism(1)
  }
}