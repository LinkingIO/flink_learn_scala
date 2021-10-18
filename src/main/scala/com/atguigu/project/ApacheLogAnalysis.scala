package com.atguigu.project

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import java.text.SimpleDateFormat

// 创建样例类TypeInformation
case class ApacheLogEvent(ip: String, userId: String, eventTime: Long, method: String, url: String)
case class UrlViewCount(url: String, WindowEnd: Long, count: Long)
object ApacheLogAnalysis {
  def main(args: Array[String]): Unit = {
    /**
     * 基于服务器log的热门页面浏览量统计
     */
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    val stream = env.readTextFile("/Users/carves/Documents/projects/flink_learn_scala/src/main/resources/apache.log")
      .map(line => {
        val arr = line.split(" ")
        // 时间解析
        val simpleDateFormat = new SimpleDateFormat("dd/MM/yyyy:HH:mm:ss")
        val ts = simpleDateFormat.parse(arr(3)).getTime

        // 83.149.9.216 - - 17/05/2015:10:05:03 +0000 GET /presentations/logstash-monitorama-2013/images/kibana-search.png
        ApacheLogEvent(arr(0), arr(2), ts, arr(5), arr(6))

      })
      // 设置一个时间戳水位线
      .assignTimestampsAndWatermarks(
        // 设置越界时间
        new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(1)) {
          override def extractTimestamp(t: ApacheLogEvent): Long = {
            t.eventTime
          }
        }
      )
//      .keyBy(_.url)
//      .timeWindow(Time.minutes(1), Time.seconds(5))
//      .aggregate(new CountAgg, new WindowResult)
//      .keyBy(_.windowEnd)
//      .process(new TopNUrl)

    stream.print()
    env.execute()

  }

  // 输入， 累加器， 输出
  class CountAgg extends AggregateFunction[ApacheLogEvent, Long, Long] {
    // 累加器
    override def createAccumulator(): Long = 0L

    override def add(in: ApacheLogEvent, acc: Long): Long = acc

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }


  class WindowResult extends WindowFunction[Long, UrlViewCount, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = ???
  }
}

