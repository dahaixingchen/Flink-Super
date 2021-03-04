package com.msb.stream.cep

import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala.{OutputTag, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

import scala.collection.Map



//第一次使用状态编程
object OrderMonitorByCEP {

  //在京东里面，一个订单创建之后，15分钟内如果没有支付，会发送一个提示信息给用户，
  // 如果15分钟内已经支付的，需要发一个提示信息给商家
  def main(args: Array[String]): Unit = {

    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    import org.apache.flink.streaming.api.scala._

    //创建一个侧输出流的Tag
    val tag =new OutputTag[OrderMessage]("pay_timeout")

    val stream: DataStream[OrderInfo] = env.readTextFile(getClass.getResource("/OrderLog.csv").getPath)
      .map(line => {
        val arr: Array[String] = line.split(",")
        new OrderInfo(arr(0), arr(1), arr(2), arr(3).toLong)
      }).assignAscendingTimestamps(_.actionTime * 1000L)

    //定义Pattern
    val pattern: Pattern[OrderInfo, OrderInfo] = Pattern.begin[OrderInfo]("begin")
      .where(_.status.equals("create"))
      .followedBy("second")
      .where(_.status.equals("pay"))
      .within(Time.minutes(15))

    //检测事件流
    val pp: PatternStream[OrderInfo] = CEP.pattern(stream.keyBy(_.oid),pattern)

    //选择数据
    val mainStream: DataStream[OrderMessage] = pp.select(tag)(
      (map: Map[String, Iterable[OrderInfo]], time: Long) => { //支付超时的数据处理
        val order: OrderInfo = map.get("begin").get.iterator.next()
        new OrderMessage(order.oid, "该订单在15分钟内没有支付，请尽快支付！", order.actionTime, 0)
      }
    )(
      (map: Map[String, Iterable[OrderInfo]]) => { //在15分钟内正常支付的订单数据处理
        val create: OrderInfo = map.get("begin").get.iterator.next()
        val pay: OrderInfo = map.get("second").get.iterator.next()
        new OrderMessage(create.oid, "订单正常支付，请经过发货!", create.actionTime, pay.actionTime)
      }
    )

    mainStream.getSideOutput(tag).print("侧流")
    mainStream.print("主流")

    env.execute()
  }

}
