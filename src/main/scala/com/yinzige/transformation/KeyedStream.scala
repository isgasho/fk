package com.yinzige.transformation

import org.apache.flink.api.common.functions.ReduceFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.scala.{
  DataStream,
  StreamExecutionEnvironment
}

//
// 2. Keyed-Stream transformation
//
object KeyedStream {
  private val env = StreamExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {
    reduce()
    env.execute()
  }

  // 2. reduce
  def reduce(): Unit = {
    val in: DataStream[(String, List[String])] = // Tuple2 stream
      env.fromElements(
        ("en", List("tea", "cola")),
        ("fr", List("vin")),
        ("en", List("cafe")),
        ("fr", List("ok"))
      )

    val out: DataStream[(String, List[String])] = in
      .keyBy(_._1)
      // .reduce(new LangReduceFunction) // ok
      .reduce((l1, l2) =>
        (l1._1, l1._2 ::: l2._2)
      ) // connect same lang words list

    // 9> (en,List(tea, cola, cafe))
    // 14> (fr,List(vin, ok))
    out.print()
  }

  class LangReduceFunction extends ReduceFunction[(String, List[String])] {
    override def reduce(
        t1: (String, List[String]),
        t2: (String, List[String])
    ): (String, List[String]) = {
      (t1._1, t1._2 ++ t2._2) // just contact 2 list with same key
    }
  }

  // 1. aggregation
  def aggregate(): Unit = {
    val in: DataStream[(Int, Int, Int)] =
      env.fromElements((1, 2, 2), (2, 3, 1), (2, 2, 4), (1, 5, 3))

    val out: DataStream[(Int, Int, Int)] = in
      .keyBy(_._1)
      .sum(1) // rolling aggregation with specific filed

    // 11> (1,2,2)
    // 16> (2,3,1)
    // 11> (1,7,2)
    // 16> (2,5,1)
    out.print()
  }
}
