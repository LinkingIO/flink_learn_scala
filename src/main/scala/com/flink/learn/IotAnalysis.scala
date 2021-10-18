package com.flink.learn

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

// 用于接收传感器温度数据的样例类
case class IotTemp(id: String, timestamp: Long, time: String, temp: Double)

// 用于接收传感器质量数据的样例类
case class IotQa(id: String, timestamp: Long, time: String, acid: Double, cons: Long, waterRate: Double)

/**
 * 1. 通过Flink dataStream 实现温度和质量的实时告警
 * 2. 通过Flink window 实现全天平均温度报告的产出
 */
object IotAnalysis {
  def main(args: Array[String]): Unit = {
    // 获取执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 文件方式读取数据
    val source = env.readTextFile("/Users/carves/Documents/projects/flink_learn_scala/src/main/resources/Iot.csv")

    // 数据流基于首2个字符进行过滤，T1代表温度，计算当天平均温度
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

    println("异常检测结果:")

    // 数据流基于首2个字符进行过滤，T1代表温度，进行实时温度告警
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
      .process(new TempWarning(5))
      .print()

    // 数据流基于首2个字符进行过滤，Q1代表质量，进行实时质量告警
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
           |温度：${strDate} ${avgTemp}
           |""".stripMargin )
    }
  }


  //  传递最大失败次数以及超出值系数
  class QaWarning(val maxOutTimes: Int, val rate: Double) extends KeyedProcessFunction[String, IotQa, String] {

    // initialize lastStates
    private var lastStates: ListState[IotQa] = _

    override def open(parameters: Configuration): Unit = {
      val last2QaDesc = new ListStateDescriptor[IotQa]("last2Qa", classOf[IotQa])
      lastStates = getRuntimeContext.getListState(last2QaDesc)
    }

    // 实时质量检测，event 出现2次质量都超过10%的变化，则产生告警
    override def processElement(value: IotQa, ctx: KeyedProcessFunction[String, IotQa, String]#Context, out: Collector[String]): Unit = {
      // 初始状态，放入第一条质量数据
      if (lastStates.get().isEmpty) {
        lastStates.add(value)
      } else if (lastStates.get().size == 1) {  // 如果状态中已经有一条质量数据，则进行质量检测，此条数据是否超过阈值
        val iotFirst = lastStates.get().toList(0)
        if (value.acid > iotFirst.acid * (1 + rate) ||
          value.cons > iotFirst.cons * (1 + rate) ||
          value.waterRate > iotFirst.waterRate * (1 + rate)
        ) {  // 超过阈值，则放入第二条质量数据
          lastStates.add(value)
        } else {  // 未超过，则清除第一条质量数据，将当前event放入
          lastStates.clear()
          lastStates.add(value)
        }
      } else if ( lastStates.get().size == 2) { // 如果状态中已经有2条质量数据，则进行质量检测，是否有连续2条数据超过阈值
        val iotFirst = lastStates.get().toList(0)
        val iotSecond = lastStates.get().toList(1)
        if (value.acid > iotFirst.acid * (1 + rate) && iotSecond.acid > iotFirst.acid * (1 + rate)) {  // 如果酸度连续2次超过阈值，则产生告警
          out.collect(
            s"""
               |${iotSecond.id},${iotSecond.time},酸度:${iotSecond.acid} 第一次酸度过高
               |${value.id},${value.time},酸度:${value.acid} 第二次酸度过高
               |""".stripMargin)
          lastStates.clear()
        }
        if (value.cons > iotFirst.cons * (1 + rate) && iotSecond.cons > iotFirst.cons * (1 + rate)) {  // 如果粘稠度连续2次超过阈值，则产生告警
          out.collect(
            s"""
               |${iotSecond.id},${iotSecond.time},酸度:${iotSecond.cons} 第一次粘稠度过高
               |${value.id},${value.time},酸度:${value.cons} 第二次粘稠度过高
               |""".stripMargin)
          lastStates.clear()
        }
        if (value.waterRate > iotFirst.waterRate * (1 + rate) && iotSecond.waterRate > iotFirst.waterRate * (1 + rate)) {  // 如果含水量连续2次超过阈值，则产生告警
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

  // 传递最大温度差
  class TempWarning(margin: Int) extends KeyedProcessFunction[String, IotTemp, String]{

    // 键控状态为mapstate，存储count和temp sum 2个key值
    private var tempState: MapState[String,Double] = _
    override def open(parameters: Configuration): Unit = {
      val msd=new MapStateDescriptor[String,Double]("mapstate",createTypeInformation[String],createTypeInformation[Double])
      tempState=getRuntimeContext.getMapState(msd)
    }

    // 实时温度检测，event 出现温度差值超过5度，则产生告警
    override def processElement(value: IotTemp, ctx: KeyedProcessFunction[String, IotTemp, String]#Context, out: Collector[String]): Unit = {
      if (tempState != null) {  // 如果已经有数据，则先判定是否产生告警，然后再更新总条数，总温度
        val avg = tempState.get("tempSum")  / tempState.get("count")
        if (value.temp > avg + margin) out.collect(s"${value.id},${value.time},${value.temp.toInt}; 温度过高")
        if (value.temp < avg - margin) out.collect(s"${value.id},${value.time},${value.temp.toInt}; 温度过低")
        tempState.put("count", tempState.get("count") + 1)
        tempState.put("tempSum", tempState.get("tempSum") + value.temp)
      } else {  // 如果没有数据，则新增第一条数据，不产生告警
        tempState.put("count", 1.0)
        tempState.put("tempSum", value.temp)
      }
    }
  }
}
