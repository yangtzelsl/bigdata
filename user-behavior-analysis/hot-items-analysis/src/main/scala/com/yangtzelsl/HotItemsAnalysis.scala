package com.yangtzelsl

import java.sql.Timestamp

import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.{TimeWindow, Window}
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

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

    stream
      .filter(_.behavior == "pv") // 过滤
        .keyBy("itemId") // 分流 KeyedStream
        // 滑动窗口 长度1h，每5m WindowedStream
        .timeWindow(Time.hours(1), Time.minutes(5))
      //.apply()
        .aggregate(new CountAgg(), new WindowResultFunction()) // 窗口聚合 （有一个预先的操作）
        .keyBy("windowEnd") // 再次keyBy
        .process(new TopNHotItems(3))
        .print()


    env.execute("HotItemsAnalysis")
  }

  /**
    * 自定义实现聚合函数
    * @UserBehavior：输入数据类型
    * @Long：累加器
    * @Long：输出数据类型
    */
  class CountAgg() extends AggregateFunction[UserBehavior, Long, Long] {

    override def createAccumulator(): Long = 0L

    override def add(in: UserBehavior, acc: Long): Long = acc + 1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  /**
    * 自定义实现WindowFunction
    * @Long：输入数据类型
    * @ItemViewCount：输出数据类型
    * @Tuple：KEY的类型
    * @TimeWindow：时间窗口
    */
  class WindowResultFunction() extends WindowFunction[Long, ItemViewCount, Tuple, TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {

      val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
      val count = input.iterator.next()

      out.collect(ItemViewCount(itemId, window.getEnd, count))
    }
  }

  class TopNHotItems(topN: Int) extends KeyedProcessFunction[Tuple, ItemViewCount, String]{

    // 定义状态ListState
    private var itemState: ListState[ItemViewCount] = _

    override def open(parameters: Configuration): Unit = {

      super.open(parameters)
      // 命名状态变量的名字和类型
      val itemStateDesc = new ListStateDescriptor[ItemViewCount]("itemState", classOf[ItemViewCount])
      itemState = getRuntimeContext.getListState(itemStateDesc)
    }

    override def processElement(i: ItemViewCount, context: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, collector: Collector[String]): Unit = {

      itemState.add(i)
      // 注册定时器，触发时间定为 windowEnd+1，触发时说明window已经收集完成所有数据
      context.timerService().registerEventTimeTimer(i.windowEnd+1)
    }

    // 定时器触发操作，从state里取出所有数据，排序取TopN，输出
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      super.onTimer(timestamp, ctx, out)

      // 获取所有的商品点击信息
      val allItems: ListBuffer[ItemViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for (item <- itemState.get){
        allItems += item
      }

      // 清除状态中的数据，释放空间
      itemState.clear()

      // 安装点击量从大到小排序
      val sortedItems = allItems
        .sortBy(_.count)(Ordering.Long.reverse)
        .take(topN)

      // 将排名数据格式化，便于打印输出
      val result: StringBuilder = new StringBuilder
      result.append("====================================\n")
      result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")

      for( i <- sortedItems.indices ){
        val currentItem: ItemViewCount = sortedItems(i)
        // 输出打印的格式 e.g.  No1：  商品ID=12224  浏览量=2413
        result.append("No").append(i+1).append(":")
          .append("  商品ID=").append(currentItem.itemId)
          .append("  浏览量=").append(currentItem.count).append("\n")
      }
      result.append("====================================\n\n")
      // 控制输出频率
      Thread.sleep(1000)

      out.collect(result.toString)
    }
  }

}
