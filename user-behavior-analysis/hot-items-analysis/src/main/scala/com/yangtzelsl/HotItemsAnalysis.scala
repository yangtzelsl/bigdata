package com.yangtzelsl

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.java.tuple.Tuple
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.apache.flink.util.Collector

// 输入数据样例类
case class UserBehavior(userId:Long, itemId:Long, categoryId:Int, behavior:String, timestamp:Long)

// 输出数据样例类
case class ItemViewCount(itemId:Long, windowEnd:Long, count: Long)

object HotItemsAnalysis {
  def main(args: Array[String]): Unit = {
    // 创建执行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // 显示定义Time类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    // 设置并发
    env.setParallelism(1)

    import org.apache.flink.api.scala._
    val filePath = "E:\\IDEA2018\\study\\user-behavior-analysis\\hot-items-analysis\\src\\main\\resources\\UserBehavior.csv"
    val stream = env.readTextFile(filePath)
      .map(line => {
        val lineArray = line.split(",")

        UserBehavior(lineArray(0).toLong, lineArray(1).toLong, lineArray(2).toInt, lineArray(3).toString, lineArray(4).toLong)
      })
      // 指定时间戳(毫秒)和watermark .assignTimestampsAndWatermarks()
      .assignAscendingTimestamps(_.timestamp * 1000)

    stream.filter(_.behavior == "pv")
        .keyBy("itemId")
        // 滑动窗口 长度1h，每5m
        .timeWindow(Time.hours(1), Time.minutes(5))
      //.apply()
        .aggregate(new CountAgg(), new WindowResultFunction())


    env.execute("HotItemsAnalysis")
  }

  /**
    * 自定义实现聚合函数
    */
  class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {

    override def createAccumulator(): Long = 0L

    override def add(in: UserBehavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  /**
    * 自定义实现WindowFunction
    */
  class WindowResultFunction() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = ???
  }

}
