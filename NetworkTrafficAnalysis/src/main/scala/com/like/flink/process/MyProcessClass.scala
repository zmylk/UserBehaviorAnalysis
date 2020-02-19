package com.like.flink.process

import java.sql.Timestamp

import com.like.flink.NetworkTraffic.UrlViewCount
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object MyProcessClass {
  class TopNHotUrls(topicSize: Int) extends KeyedProcessFunction[Long,UrlViewCount,String] {
    lazy val urlState: ListState[UrlViewCount] = getRuntimeContext.getListState(new ListStateDescriptor[UrlViewCount]("urlState", classOf[UrlViewCount]))

    override def processElement(value: UrlViewCount, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#Context, out: Collector[String]): Unit = {
      urlState.add(value)
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 10 * 1000)
    }

    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, UrlViewCount, String]#OnTimerContext, out: Collector[String]): Unit =
    {
      val allUrl: ListBuffer[UrlViewCount] = ListBuffer()
      import scala.collection.JavaConversions._
      for (element <-urlState.get()){
        allUrl.add(element)
      }
      urlState.clear()
      val sortedItems: ListBuffer[UrlViewCount] = allUrl.sortBy(_.count)(Ordering.Long.reverse).take(topicSize)


      // 将排名数据格式化，便于打印输出
      val result: StringBuilder = new StringBuilder
      result.append("====================================\n")
      result.append("时间：").append(new Timestamp(timestamp - 1)).append("\n")

      for( i <- sortedItems.indices ){
        //val currentItem: ItemViewCount = sortedItems(i)
        val currentItem: UrlViewCount = sortedItems(i)
        // 输出打印的格式 e.g.  No1：  商品ID=12224  浏览量=2413
        result.append("No").append(i+1).append(":")
          .append("  商品ID=").append(currentItem.url)
          .append("  浏览量=").append(currentItem.count).append("\n")
      }
      result.append("====================================\n\n")
      // 控制输出频率
      Thread.sleep(1000)
      out.collect(result.toString)

    }
  }



}
