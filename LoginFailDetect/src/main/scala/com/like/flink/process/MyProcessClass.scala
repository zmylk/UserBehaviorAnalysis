package com.like.flink.process

import java.sql.Timestamp

import com.like.flink.LoginFail.LoginEvent
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

object MyProcessClass {

  class TopNHotUrls extends KeyedProcessFunction[Long,LoginEvent,String] {

    lazy val loggerState: ListState[LoginEvent] = getRuntimeContext.getListState(new ListStateDescriptor[LoginEvent]("loggerState",classOf[LoginEvent]))

    override def processElement(value: LoginEvent, ctx: KeyedProcessFunction[Long, LoginEvent, String]#Context, out: Collector[String]): Unit = {
      //添加数据
      loggerState.add(value)
      ctx.timerService().registerEventTimeTimer(value.eventTime + 2*1000)
    }


    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Long, LoginEvent, String]#OnTimerContext, out: Collector[String]): Unit = {
      var allLogger: ListBuffer[LoginEvent] = ListBuffer()

      import scala.collection.JavaConversions._
      for (element <- loggerState.get())
      {
          allLogger.add(element)
      }
      loggerState.clear()
      if (allLogger.size > 1)
        {
          out.collect(allLogger(0).toString())
        }
    }

  }
}
