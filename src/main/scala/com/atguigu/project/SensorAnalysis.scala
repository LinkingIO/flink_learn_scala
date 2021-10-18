package com.atguigu.project


import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.util.Collector

object SensorAnalysis {

  val tmp_output = new OutputTag[String]("tmp-output")
  private val qa_output = new OutputTag[String]("qa-output")

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    val stream = env.fromElements(
      SensorReading("sensor_1", 1547718199, 35.80018327300259),
      SensorReading("sensor_6", 1547718199, 15.402984393403084),
      SensorReading("sensor_6", 1547718199, 6.720945201171228),
      SensorReading("sensor_10", 1547718199, 38.101067604893444))

    stream.filter(_.id.equals("sensor_6")).print()
    val warnings = stream.process(new tempAlarm)
//    stream.keyBy(_.id.equals("sensor_6")).sum(2).print()
//    warnings.getSideOutput(tmp_output).print()
//    warnings.getSideOutput(qa_output).print()

    env.execute()
  }
  class tempAlarm extends ProcessFunction[SensorReading, SensorReading] {
    override def processElement(i: SensorReading, context: ProcessFunction[SensorReading, SensorReading]#Context, collector: Collector[SensorReading]): Unit = {
      if (i.id.equals("sensor_6")) {
        context.output(tmp_output, "6传感器id：" + i.id )
      }
      if (i.id.equals("sensor_1")) {
        context.output(qa_output, "1传感器id：" + i.id)
      }
      collector.collect(i)
    }
  }

}

case class SensorReading(id: String, timestamp: Int, temper: Double)

