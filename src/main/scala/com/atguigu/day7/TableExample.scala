package com.atguigu.day7

import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.table.api.{DataTypes, EnvironmentSettings, Table}
import org.apache.flink.table.api.scala._
import org.apache.flink.table.descriptors.{Csv, FileSystem, Schema}
import org.apache.flink.types.Row


case class SensorReading(id: String, timestamp: Long, temper: Double)

object TableExample {
  def main(args: Array[String]): Unit = {
    // 初始化环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 初始化环境配置，创建planner
    val settings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner() // 使用blink planner， 流批一体
      .inStreamingMode()
      .build()

    // 初始化表环境
    val tEnv = StreamTableEnvironment.create(env, settings)

        // 扫描注册订单表
        tEnv.connect(new FileSystem().path("/Users/carves/Documents/projects/flink_learn_scala/src/main/resources/sensor.txt"))
          .withFormat(new Csv())
          .withSchema(
            new Schema()
              .field("id", DataTypes.STRING())
              .field("timestamp", DataTypes.BIGINT())
              .field("temperature", DataTypes.DOUBLE())
          ).createTemporaryTable("inputTable")  // 创建临时表

        // 将临时表转换成Table
        val sensorTable: Table = tEnv.from("inputTable")

        // 使用table api进行查询
        val result = sensorTable
          .select("id, temperature")
          .filter("id  = 'sensor_1'")

        tEnv.toAppendStream[Row](result).print()

        // 使用sql api查询
        val sqlResult = tEnv.sqlQuery("select id, temperature from inputTable where id = 'sensor_1'")

        tEnv.toAppendStream[Row](sqlResult).print()

//    // 读取inputStream
//    val input = env.readTextFile("/Users/carves/Documents/projects/flink_learn_scala/src/main/resources/sensor.txt")
//
//    val sensorStream: DataStream[SensorReading] = input.map(line => {
//      val lineArray = line.split(",")
//      SensorReading(lineArray(0), lineArray(1).toLong, lineArray(2).toDouble)
//    })
//
//    val sensorTable = tEnv.fromDataStream(sensorStream)
//    val sensorTable2 = tEnv.fromDataStream(sensorStream, 'id, 'timestamp as 'ts)
//
//    val tableS = sensorTable
//      .groupBy('id)
//      .select('id, 'id.count as 'count)
//    tEnv.toAppendStream[Row](tableS).print()
//val exp = tEnv.explain(tableS)
//    println(exp)
//
//    // 生成SensorReading对应的dataStream
//
//    // 创建表

    env.execute()
  }
}
