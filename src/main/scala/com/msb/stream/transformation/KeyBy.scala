package com.msb.stream.transformation

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._

object KeyBy {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val sourStream: DataStream[Long] = env.generateSequence(1, 10)
    val mapData: DataStream[(Long, Int)] = sourStream.map(x => (x % 3, 1))
    mapData.print()
    mapData.keyBy(k =>
      k._1
    ).reduce((v1,v2)=>(v1._1,v1._2+v2._2)).print()

    env.execute()
  }

}
