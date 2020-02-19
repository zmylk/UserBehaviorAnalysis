package com.like.flink

import java.text.SimpleDateFormat
import com.like.flink.aggregate.MyAggregateClass.{CountAgg, WindowResultFunction}
import com.like.flink.process.MyProcessClass.TopNHotUrls
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{DataStream, KeyedStream, StreamExecutionEnvironment, WindowedStream}
import org.apache.flink.api.scala._
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
object NetworkTraffic {
  case class ApacheLogEvent( ip: String, userId: String, eventTime: Long, method: String, url:String)
  case class UrlViewCount( url: String, windowEnd: Long, count: Long)
  def main(args: Array[String]): Unit = {

    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //读取数据
    val startStram: DataStream[String] = env.readTextFile("D:\\mycode\\atguigu\\Flink\\UserBehaviorAnalysis\\NetworkTrafficAnalysis\\src\\main\\resources\\apachetest.log")
    //转换为指定类型
    val loggerSteam: DataStream[ApacheLogEvent] = startStram.map(line => {
      val strings: Array[String] = line.split(" ")
      val time: Long = new SimpleDateFormat("dd/MM/yyyy:hh:mm:ss").parse(strings(3)).getTime
      ApacheLogEvent(strings(0), strings(1), time, strings(5), strings(6))
    })
    val setWatermarksStream: DataStream[ApacheLogEvent] = loggerSteam.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[ApacheLogEvent](Time.seconds(10)) {
      override def extractTimestamp(element: ApacheLogEvent): Long = {
        element.eventTime
      }
    })
    val keyByStram: KeyedStream[ApacheLogEvent, String] = setWatermarksStream.keyBy(_.ip)
    val windowStream: WindowedStream[ApacheLogEvent, String, TimeWindow] = keyByStram.timeWindow(Time.minutes(1),Time.seconds(5))
    val aggStream: DataStream[UrlViewCount] = windowStream.aggregate(new CountAgg,new WindowResultFunction)
    val byWindowStream: KeyedStream[UrlViewCount, Long] = aggStream.keyBy(_.windowEnd)
    val endStram: DataStream[String] = byWindowStream.process(new TopNHotUrls(3))
    endStram.print()
    env.execute()

  }

}
