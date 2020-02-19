package com.like.flink
import java.sql.Timestamp

import com.like.flink.util.MyKafkaUtil
import org.apache.flink.api.common.functions.AggregateFunction
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.java.tuple.{Tuple, Tuple1}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

import scala.collection.mutable.ListBuffer

//输入数据样例类
case class UserBehavior( userId: Long, itemId: Long, categoryId: Int, behavior: String, timestamp: Long)
//输出数据样例类
case class ItemViewCount(itemId: Long, windowEnd: Long, count: Long )
object HotItems {
  def main(args: Array[String]): Unit = {
    //创建一个env
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    // 显示的定义time类型
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.setParallelism(1)

    //读取流
//    val startStram: DataStream[String] = env.readTextFile("D:\\mycode\\atguigu\\Flink\\UserBehaviorAnalysis\\HotItemsAnalysis\\src\\main\\resources\\UserBehavior.csv")

    val startStram: DataStream[String] = env.addSource(MyKafkaUtil.getConsumer("test"))
    startStram.print()
    //加工数据
    val userBehaviorDs: DataStream[UserBehavior] = startStram.map(line => {
      val oneEvent: Array[String] = line.split(",")
      UserBehavior(oneEvent(0).toLong, oneEvent(1).toLong, oneEvent(2).toInt, oneEvent(3), oneEvent(4).toLong)
    })


    //指定时间戳和watermark
//    val setAssignDs: DataStream[UserBehavior] = userBehaviorDs.assignAscendingTimestamps(_.timestamp * 1000)
        val setAssignDs: DataStream[UserBehavior] = userBehaviorDs.assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[UserBehavior](Time.milliseconds(1)) {
          override def extractTimestamp(t: UserBehavior): Long = {
            t.timestamp *1000
          }
        })

    val keyByDs: KeyedStream[UserBehavior, Tuple] = setAssignDs.filter(_.behavior == "pv").keyBy("itemId")
    val windowStream: WindowedStream[UserBehavior, Tuple, TimeWindow] = keyByDs.timeWindow(Time.hours(1),Time.minutes(5))
    val reduceDS: DataStream[ItemViewCount] = windowStream.aggregate(new CountAgg(), new WindowResultFunction())

    val keyStream: KeyedStream[ItemViewCount, Tuple] = reduceDS.keyBy("windowEnd")

    val resultDs: DataStream[String] = keyStream.process(new TopNHotItems(3))
    resultDs.print()
    env.execute()
  }

  class TopNHotItems(topSize: Int) extends KeyedProcessFunction[Tuple,ItemViewCount,String] {
    //定义ListState
    private var itemState: ListState[ItemViewCount] = _

    //收集数据
    override def open(parameters: Configuration): Unit = {
      super.open(parameters)
      // 命名状态变量的名字和类型
      val itemSateUnit: ListStateDescriptor[ItemViewCount] = new ListStateDescriptor[ItemViewCount]("itemState",classOf[ItemViewCount])
      itemState = getRuntimeContext.getListState(itemSateUnit)
    }

    //注册触发时间
    override def processElement(value: ItemViewCount, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#Context, out: Collector[String]): Unit = {
      itemState.add(value)
      // 注册定时器，触发时间定位windowEnd + 1,触发时说明window已经收集完所有的数据
      ctx.timerService().registerEventTimeTimer(value.windowEnd + 1)
    }

    //触发事件
    override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[Tuple, ItemViewCount, String]#OnTimerContext, out: Collector[String]): Unit = {
      //获取所有商品的点击信息
      val allItems: ListBuffer[ItemViewCount] = ListBuffer()
      import  scala.collection.JavaConversions._
      val size: Int = itemState.get().size
      for(item <-itemState.get)
        {
          allItems += item
        }
      // 清除状态中的数据，释放空间

      itemState.clear()
      // 按照点击量从大到小排序，选取TopN
      val sortedItems = allItems.sortBy(_.count)(Ordering.Long.reverse).take(topSize)

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


  class  CountAgg extends AggregateFunction[UserBehavior,Long,Long] {
    override def createAccumulator(): Long = 0L

    override def add(in: UserBehavior, acc: Long): Long = acc +1

    override def getResult(acc: Long): Long = acc

    override def merge(acc: Long, acc1: Long): Long = acc + acc1
  }

  class WindowResultFunction extends WindowFunction[Long,ItemViewCount,Tuple,TimeWindow] {
    override def apply(key: Tuple, window: TimeWindow, input: Iterable[Long], out: Collector[ItemViewCount]): Unit = {
      val itemId: Long = key.asInstanceOf[Tuple1[Long]].f0
      val count = input.iterator.next()
      out.collect(ItemViewCount(itemId,window.getEnd,count))
    }
  }

}
