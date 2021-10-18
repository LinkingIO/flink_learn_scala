package com.atguigu.project

import com.sun.jmx.snmp.Timestamp
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.ListStateDescriptor
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.api.scala.typeutils.Types
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.ProcessWindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer


case class UserLog(userId: Long, itemId: Long, categoryId: Int, behavior: String,timestamp: Long )
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long)


object HotItems {
  /**
   *
   * @param args
   */
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(2)

    val stream = env.readTextFile("/Users/carves/Documents/projects/flink_learn_scala/src/main/resources/UserBehavior.csv")
      .map(line => {
        val lineArray = line.split(",")
        UserLog(lineArray(0).toLong, lineArray(1).toLong, lineArray(2).toInt, lineArray(3), lineArray(4).toLong)
      })
      .assignAscendingTimestamps(_.timestamp * 1000)
      .filter(_.behavior.equals("pv"))
      .keyBy(_.itemId)
      .timeWindow(Time.hours(1), Time.minutes(5))
      .aggregate(new AggCount(), new WindowResultFunction())
      .keyBy(_.windowEnd)
      .process(new TopNHotItems(3))

    stream.print()

    env.execute()
  }
}

class TopNHotItems(topSize: Int)
  extends KeyedProcessFunction[Long, ItemViewCount, String] {
  // 惰性赋值一个状态变量
  lazy val itemState = getRuntimeContext.getListState(
    new ListStateDescriptor[ItemViewCount]("items", Types.of[ItemViewCount])
  )

  // 来一条数据都会调用一次
  override def processElement(value: ItemViewCount,
                              ctx: KeyedProcessFunction[Long,
                                ItemViewCount, String]#Context,
                              out: Collector[String]): Unit = {
    itemState.add(value)
    ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
  }

  // 定时器事件
  override def onTimer(
                        ts: Long,
                        ctx: KeyedProcessFunction[Long, ItemViewCount, String]#OnTimerContext,
                        out: Collector[String]
                      ): Unit = {
    val allItems: ListBuffer[ItemViewCount] = ListBuffer()
    // 导入一些隐式类型转换
    import scala.collection.JavaConversions._
    for (item <- itemState.get) {
      allItems += item
    }

    // 清空状态变量，释放空间
    itemState.clear()

    // 降序排列
    val sortedItems = allItems.sortBy(-_.count).take(topSize)
    val result = new StringBuilder
    result.append("====================================\n")
    result.append("时间: ").append(new Timestamp(ts - 1)).append("\n")
    for (i <- sortedItems.indices) {
      val currentItem = sortedItems(i)
      result.append("No")
        .append(i + 1)
        .append(":")
        .append("  商品ID=")
        .append(currentItem.itemId)
        .append("  浏览量=")
        .append(currentItem.count)
        .append("\n")
    }
    result.append("====================================\n\n")
    Thread.sleep(1000)
    out.collect(result.toString())
  }
}

class AggCount() extends AggregateFunction[UserLog, Long, Long] {
  override def createAccumulator() = 0L

  override def add(in: UserLog, acc: Long): Long = acc + 1

  override def getResult(acc: Long): Long = acc

  override def merge(acc: Long, acc1: Long): Long = acc1 + acc
}

// ?
class WindowResultFunction() extends ProcessWindowFunction[Long, ItemViewCount, Long, TimeWindow] {
  override def process(key: Long, context: Context, elements: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
    out.collect(ItemViewCount(key, context.window.getEnd, elements.iterator.next()))
  }
}

