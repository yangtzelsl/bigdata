package com.yangtzelsl.util

import java.util.Properties

import com.yangtzelsl.util.FlinkSinkUtils.dataStream
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment, _}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.datastream.DataStreamSink
import org.apache.flink.streaming.connectors.kafka.{FlinkKafkaConsumer011, FlinkKafkaProducer011}
// 定义样例类，温度传感器
case class SensorReading( id: String, timestamp: Long, temperature: Double )

object FlinkSinkUtils {

  def kafkaSink(dataStream: DataStream[T]): DataStreamSink[T] ={
    val brokerList = "localhost:9092"
    val topicId = "sinktest"
    dataStream.addSink( new FlinkKafkaProducer011[T](brokerList, topicId, new SimpleStringSchema()))
  }


  val env = StreamExecutionEnvironment.getExecutionEnvironment
  env.setParallelism(1)
  // 读取数据
  //    val inputPath = "D:\\Projects\\BigData\\FlinkTutorial\\src\\main\\resources\\sensor.txt"
  //    val inputStream = env.readTextFile(inputPath)

  // 从kafka读取数据
  val properties = new Properties()
  properties.setProperty("bootstrap.servers", "localhost:9092")
  properties.setProperty("group.id", "consumer-group")
  val stream = env.addSource( new FlinkKafkaConsumer011[String]("sensor", new SimpleStringSchema(), properties) )


  // 先转换成样例类类型（简单转换操作）
  val dataStream = stream
    .map( data => {
      val arr = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble).toString
    } )

  dataStream.addSink( new FlinkKafkaProducer011[String]("localhost:9092", "sinktest", new SimpleStringSchema()) )

  env.execute("kafka sink test")

}
