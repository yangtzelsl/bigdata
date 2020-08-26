package com.yangtzelsl

import org.apache.flink.cep.scala.CEP
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

// 定义输入的订单事件流
case class OrderEvent(orderId: Long, eventType: String, eventTime: Long)

// 定义输出的结果
case class OrderResult(orderId: Long, eventType: String)

object OrderTimeoutDetect {

  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val orderEventStream = env.fromCollection(List(
      OrderEvent(1, "create", 1558430842),
      OrderEvent(2, "create", 1558430843),
      OrderEvent(1, "pay", 1558436842),
      OrderEvent(2, "pay", 1558430844)
    ))
      .assignAscendingTimestamps(_.eventTime * 1000)
      .keyBy(_.orderId)

    // 定义pattern
    val orderPayPattern = Pattern
      .begin[OrderEvent]("begin")
      .where(_.eventType == "create")
      .followedBy("follow")
      .where(_.eventType == "pay")
      .within(Time.minutes(15))

    // 定义一个输出标签，用于标明 侧输出流 side stream
    val orderTimeoutOutputTag = OutputTag[OrderResult]("orderTimeout")

    // 从keyBy之后的每条流中匹配定义好的模式，得到pattern stream
    val patternStream = CEP.pattern(orderEventStream, orderPayPattern)

    import scala.collection.Map
    // 从patternStream中获得输出流
    // select(patternTimeoutFun, patternSelectFun)
    // select(outputTag, patternTimeoutFun, patternSelectFun)
    val completedResultDataStream = patternStream.select(orderTimeoutOutputTag)(
      // 对于超时的序列部分，调用 pattern timeout function
      (pattern: Map[String, Iterable[OrderEvent]], timestamp:Long) => {
        //
        val timeoutOrderId = pattern
        .getOrElse("begin",null)
        .iterator
        .next()
        .orderId

        OrderResult(timeoutOrderId, "timeout")
    }
    )(// 柯里化编程
      (pattern:Map[String, Iterable[OrderEvent]]) => {
        // 获取支付成功的订单
        val payedOrderId = pattern
          .getOrElse("follow",null)
          .iterator
          .next()
          .orderId

        OrderResult(payedOrderId, "success")
      }
    )

    completedResultDataStream.print()// 目前为止 打印出来的 是匹配的事件序列

    // 打印输出timeout结果
    val timeoutResultDataStream = completedResultDataStream.getSideOutput(orderTimeoutOutputTag)
    timeoutResultDataStream.print()

    env.execute("OrderTimeoutDetect")
  }

}
