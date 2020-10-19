package com.yangtzelsl.stream

import org.apache.flink.streaming.api.functions.source.SourceFunction

import scala.util.Random

class MySource extends SourceFunction[Double]{

  // 定义标志位
  var flag: Boolean = true

  override def run(ctx: SourceFunction.SourceContext[Double]): Unit = {

    // 随机
    val random = new Random()

    // 定义无限循环，不停产生数据，除非被cancel
    while (flag) {
      ctx.collect(2)
    }
  }

  /**
    * 数据源 结束产生的标志
    */
  override def cancel(): Unit = flag = false
}
