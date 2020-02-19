package com.like.flink

import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, OutputTag, StreamExecutionEnvironment}
import org.apache.flink.api.scala._
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.streaming.api.windowing.time.Time
object OrderTimeout {
  //定义输入的订单事件流
  case class OrderEvent( orderId: Long, eventType: String, eventTimeL:Long)
  //定义输出结果
  case class OrderResult( orderId: Long, eventType: String)
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)
    
    val orderEventStream = env.fromCollection(List(
      OrderEvent(1, "create", 1558430842),
      OrderEvent(2, "create", 1558430843),
      OrderEvent(1, "pay", 1558436842),
      OrderEvent(2, "pay", 1558430844)
    ))
      .assignAscendingTimestamps(_.eventTimeL * 1000)
      .keyBy(_.orderId)
    
    //定义Pattern
    val orderPattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("begin").where(_.eventType == "create").followedBy("follow").where(_.eventType == "pay")
      .within(Time.minutes(15))
    //定义一个输出标签
    val orderTimeOutput: OutputTag[OrderResult] = OutputTag[OrderResult]("orderTimeout")
    
    //从定义好的匹配模式中获取数据
    val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream,orderPattern)

    import scala.collection.Map
    val completedResultDataStream: DataStream[OrderResult] = patternStream.select(orderTimeOutput)(
      (pattern: Map[String, Iterable[OrderEvent]], timestamp: Long) => {
        val begain: OrderEvent = pattern.getOrElse("begin", null).iterator.next()
        println(timestamp)
        OrderResult(begain.orderId, "timeout")
      }
    )(
      (pattern: Map[String, Iterable[OrderEvent]]) => {
        val id: Long = pattern.getOrElse("follow", null).iterator.next().orderId
        OrderResult(id, "success")
      }
    )
    completedResultDataStream.print()
    
    //打印输出Outtime结果
    
    val timeoutResultDataStream: DataStream[OrderResult] = completedResultDataStream.getSideOutput(orderTimeOutput)
    timeoutResultDataStream.print()

    env.execute("Order Timeout Detect Job")
  }

}
