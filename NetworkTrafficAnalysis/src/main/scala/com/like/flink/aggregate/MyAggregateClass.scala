package com.like.flink.aggregate

import com.like.flink.NetworkTraffic.{ApacheLogEvent, UrlViewCount}
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

object MyAggregateClass {

  class CountAgg extends AggregateFunction[ApacheLogEvent,Long,Long] {
    override def createAccumulator(): Long = 0L

    override def add(value: ApacheLogEvent, accumulator: Long): Long = accumulator+1

    override def getResult(accumulator: Long): Long = accumulator

    override def merge(a: Long, b: Long): Long = a+b
  }

  class WindowResultFunction extends WindowFunction[Long,UrlViewCount,String,TimeWindow] {
    override def apply(key: String, window: TimeWindow, input: Iterable[Long], out: Collector[UrlViewCount]): Unit = {
      val url = key
      val count = input.iterator.next()
     out.collect(UrlViewCount(url,window.getEnd,count))
    }
  }
}
