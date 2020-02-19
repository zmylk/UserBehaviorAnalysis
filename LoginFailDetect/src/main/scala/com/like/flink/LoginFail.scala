package com.like.flink

import com.like.flink.process.MyProcessClass.TopNHotUrls
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment}

object LoginFail {
  case class LoginEvent( userId: Long,ip: String, eventType: String, eventTime: Long)
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    val loginStream = env.fromCollection(List(
      LoginEvent(1, "192.168.0.1", "fail", 1558430844),
      LoginEvent(1, "192.168.0.2", "fail", 1558430843),
      LoginEvent(1, "192.168.0.3", "fail", 1558430844),
      LoginEvent(2, "192.168.10.10", "success", 1558430845)
    ))

    val startDs: DataStream[LoginEvent] = loginStream.assignAscendingTimestamps(_.eventTime *1000)
    val byKeyDs: KeyedStream[LoginEvent, Long] = startDs.keyBy(_.userId)
    val result: DataStream[String] = byKeyDs.process(new TopNHotUrls)
    result.print()
    env.execute()
  }

}
