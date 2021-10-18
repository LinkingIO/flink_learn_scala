package com.flink.learn

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state._
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat
import scala.collection.JavaConversions.iterableAsScalaIterable

//// 用于接收传感器温度数据的样例类
//case class IotTemp(id: String, timestamp: Long, time: String, temp: Double)
//
//// 用于接收传感器质量数据的样例类
//case class IotQa(id: String, timestamp: Long, time: String, acid: Double, cons: Long, waterRate: Double)

/**
 * 1. 通过Flink dataStream 实现温度和质量的实时告警
 * 2. 通过Flink window 实现全天平均温度报告的产出
 */
object IotAnalysisReport {
  def main(args: Array[String]): Unit = {
    // 获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 文件方式读取数据
    val source = env.readTextFile("/Users/carves/Documents/projects/flink_learn_scala/src/main/resources/Iot.csv")

    println("异常检测结果:")

    // 数据流基于首2个字符进行过滤，T1代表温度，先进行温度告警，然后计算当天温度
    source.filter(line =>line.substring(0, 2).equals("T1"))
      .map(line => {
        val lineArray = line.substring(0, line.length - 1).split(",")

        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val date = dateFormat.parse(lineArray(1)).getTime

        IotTemp(lineArray(0), date, lineArray(1), lineArray(2).toDouble)   // 将数据流进行对象化
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[IotTemp](Time.seconds(1)) {  // 指定时间戳和watermark
        override def extractTimestamp(t: IotTemp): Long = t.timestamp
      })
      .keyBy(_.id)
      .timeWindow(Time.days(1))  // 将以天为window，进行聚合
      .process(new AvgProcessFunction)
      .print()

    // 数据流基于首2个字符进行过滤，Q1代表温度，先进行温度告警，然后计算当天温度
    source.filter(line =>line.substring(0, 2).equals("Q1"))
      .map(line => {
        val lineArray = line.substring(0, line.length - 1).split(",")
        var acid = 0.0
        var cons = 0
        var waterRate = 0.0
        for (ele <- lineArray) {
          if (ele.startsWith("AB:")) acid = ele.substring(3).toDouble
          if (ele.startsWith("AE:")) cons = ele.substring(3).toInt
          if (ele.startsWith("CE:")) waterRate = ele.substring(3).toDouble
        }

        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        val date = dateFormat.parse(lineArray(1)).getTime

        IotQa(lineArray(0), date, lineArray(1), acid, cons, waterRate)
      })
      .assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[IotQa](Time.seconds(1)) {
        override def extractTimestamp(element: IotQa): Long = element.timestamp
      })
      .keyBy(_.id)
      .process(new QaWarning(2, 0.1))
      .print()

    env.execute()
  }

  class TemAvgReport

  // 计算每个传感器的平均温度的增量聚合函数
  // 计算器统计的是每个event time的数据
  class AvgTempFunction  extends AggregateFunction[IotTemp,
    (String, Double, Int), (String, Double, Int)]{
    override def createAccumulator(): (String, Double, Int) = ("", 0.0, 0)

    override def add(value: IotTemp, accumulator: (String, Double, Int)): (String, Double, Int) = {
      if (accumulator != None ) {
        val avgTemp = accumulator._2 / accumulator._3
        if ( value.temp - avgTemp > 5.0) printf("%s,%s,%d; 温度过高\n", value.id, value.time, value.temp.toInt)
      }
      (value.id, value.temp + accumulator._2, 1 + accumulator._3)
    }

    override def getResult(accumulator: (String, Double, Int)): (String, Double, Int) = {
      (accumulator._1, accumulator._2, accumulator._3)
    }

    override def merge(a: (String, Double, Int), b: (String, Double, Int)): (String, Double, Int) = ???
  }


  // 编写UDF， 实现对全天温度的统计。
  class AvgProcessFunction extends ProcessWindowFunction[IotTemp, String, String, TimeWindow] {
    override def process(key: String, context: Context, elements: Iterable[IotTemp], out: Collector[String]): Unit = {
      val dateTime = context.window.getStart

      val simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")
      val strDate = simpleDateFormat.format(dateTime)

      var sumTemp = 0.0
      var count = 0
      elements.foreach( ele => {
        count += 1
        sumTemp += ele.temp
      })
      val avgTemp = (sumTemp / count).formatted("%.2f")

      out.collect(
        s"""
           |报表结果：
           |  温度：${strDate} ${avgTemp}
           |""".stripMargin )
    }
  }


  // UDF, 传递最大失败次数以及超出值系数
  class QaWarning(val maxOutTimes: Int, val rate: Double) extends KeyedProcessFunction[String, IotQa, String] {

    // initialize lastStates
    private var lastStates: ListState[IotQa] = _

    override def open(parameters: Configuration): Unit = {
      val last2QaDesc = new ListStateDescriptor[IotQa]("last2Qa", classOf[IotQa])
      lastStates = getRuntimeContext.getListState(last2QaDesc)
    }

    // process function
    override def processElement(value: IotQa, ctx: KeyedProcessFunction[String, IotQa, String]#Context, out: Collector[String]): Unit = {
      // if status list is null, add list
      if (lastStates.get().isEmpty) {
        lastStates.add(value)
      } else if (lastStates.get().size == 1) {  // if status list size = 1 ，whether current ele over the threshold
        val iotFirst = lastStates.get().toList(0)
        if (value.acid > iotFirst.acid * (1 + rate) ||
          value.cons > iotFirst.cons * (1 + rate) ||
          value.waterRate > iotFirst.waterRate * (1 + rate)
        ) {
          lastStates.add(value)
        } else {
          lastStates.clear()
        }
      } else if ( lastStates.get().size == 2) { // if status list size = 2 , 2 elements exceed threshold , process warning. else update list state
        val iotFirst = lastStates.get().toList(0)
        val iotSecond = lastStates.get().toList(1)
        if (value.acid > iotFirst.acid * (1 + rate) && iotSecond.acid > iotFirst.acid * (1 + rate)) {
          out.collect(
            s"""
               |${iotSecond.id},${iotSecond.time},酸度:${iotSecond.acid} 第一次酸度过高
               |${value.id},${value.time},酸度:${value.acid} 第二次酸度过高
               |""".stripMargin)
          lastStates.clear()
        }
        if (value.cons > iotFirst.cons * (1 + rate) && iotSecond.cons > iotFirst.cons * (1 + rate)) {
          out.collect(
            s"""
               |${iotSecond.id},${iotSecond.time},酸度:${iotSecond.cons} 第一次粘稠度过高
               |${value.id},${value.time},酸度:${value.cons} 第二次粘稠度过高
               |""".stripMargin)
          lastStates.clear()
        }
        if (value.waterRate > iotFirst.waterRate * (1 + rate) && iotSecond.waterRate > iotFirst.waterRate * (1 + rate)) {
          out.collect(
            s"""
               |${iotSecond.id},${iotSecond.time},酸度:${iotSecond.waterRate} 第一次含水量过高
               |${value.id},${value.time},酸度:${value.waterRate} 第二次含水量过高
               |""".stripMargin)
          lastStates.clear()
        }
        lastStates.clear()
        lastStates.add(value)
      }
    }
  }
}
