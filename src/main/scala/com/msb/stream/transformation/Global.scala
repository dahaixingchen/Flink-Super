package com.msb.stream.transformation

import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

object Global {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val stream: DataStream[Long] = env.generateSequence(1,10).setParallelism(2)
    stream.writeAsText("./data/stream1").setParallelism(2)
    stream.global.writeAsText("./data/stream2").setParallelism(10)
    env.execute()
  }

}
