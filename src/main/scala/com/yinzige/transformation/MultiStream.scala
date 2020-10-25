package com.yinzige.transformation

import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.functions.co.CoMapFunction
import org.apache.flink.streaming.api.scala.{
  ConnectedStreams,
  DataStream,
  StreamExecutionEnvironment
}

object MultiStream {
  private val env = StreamExecutionEnvironment.getExecutionEnvironment
  private val in1: DataStream[(Int, Long)] = env.fromElements((2, 3), (4, 5))
  private val in2: DataStream[(Int, String)] = env.fromElements((1, "ONE"), (2, "TWO"))

  def main(args: Array[String]): Unit = {
    keyConnect()
    env.execute()
  }

  // 3, key connect, broadcast
  def keyConnect(): Unit = {
    val connected1: ConnectedStreams[(Int, Long), (Int, String)] = in1
      .connect(in2)
      .keyBy(0, 0) // keyBy 2 connected stream

    val connected2 = in1.keyBy(_._1).connect(in2.keyBy(_._1)) // connect 2 keyed stream

    /**
      * 11> `ONE`
      * 1> <20>
      * 16> <6>
      * 16> `TWOTWO`
      */
    connected1.map(new coMapFunc).print()
  }

  // 2. connect
  def connect(): Unit = {
    val connected: ConnectedStreams[(Int, Long), (Int, String)] = in1.connect(in2)
    val out: DataStream[String] = connected.map(new coMapFunc)

    /**
      * 14> `ONE`
      * 3> <6>
      * 15> `TWOTWO`
      * 4> <20>
      */
    out.print()
  }

  // map to result type respectively
  class coMapFunc extends CoMapFunction[(Int, Long), (Int, String), String] {
    override def map1(in1: (Int, Long)): String = "<" + in1._1 * in1._2 + ">"
    override def map2(in2: (Int, String)): String = "`" + in2._2 * in2._1 + "`"
  }

  // 1. union
  def union(): Unit = {
    val in1: DataStream[String] = env.fromElements("a", "b")
    val in2: DataStream[String] = env.fromElements("1")
    val in3: DataStream[String] = env.fromElements("A")
    val out = in1.union(in2, in3)

    /**
      * 7> 1
      * 5> a
      * 15> A
      * 6> b
      */
    out.print()
  }
}
