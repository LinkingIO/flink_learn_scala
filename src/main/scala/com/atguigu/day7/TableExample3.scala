package com.atguigu.day7

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala.StreamTableEnvironment
import org.apache.flink.table.api.scala._


/**
 * 流处理中时间特性
 */

object TableExample3 {
  case class SensorReading(id: String, timestamp: Long, temperature: Double)
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 初始化环境配置，创建planner
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner() // 使用blink planner， 流批一体
      .inStreamingMode()
      .build()

    // 初始化表环境
    val tEnv = StreamTableEnvironment.create(env, settings)

    // 实现对数据读取
    val input = env.readTextFile("/Users/carves/Documents/projects/flink_learn_scala/src/main/resources/sensor.txt")

    val sensorStream: DataStream[SensorReading] = input.map(line => {
      val lineArray = line.split(",")
      SensorReading(lineArray(0), lineArray(1).toLong, lineArray(2).toDouble)
    })

    // 'pt.proctime 定义了处理时间，必须放在最后
    tEnv.fromDataStream(sensorStream, 'id, 'timestamp, 'temperature, 'pt.proctime)

    val sinkDDL: String =
      """
        |create table dataTable (
        |  id varchar(20) not null,
        |  ts bigint,
        |  temperature double,
        |  pt AS PROCTIME()
        |) with (
        |  'connector.type' = 'filesystem',
        |  'connector.path' = 'sensor.txt',
        |  'format.type' = 'csv'
        |)
  """.stripMargin

    tEnv.sqlUpdate(sinkDDL) // 执行 DDL， 1.11 后被废弃


  }
}
