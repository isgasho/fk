package com.yinzige.transformation

import com.yinzige.sensor.SensorReading
import org.apache.flink.api.common.functions.{FilterFunction, FlatMapFunction, MapFunction}
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.util.Collector
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}

//
// 1. Basic transformation
//
object Basic {
  private val env = StreamExecutionEnvironment.getExecutionEnvironment

  def main(args: Array[String]): Unit = {
    flat()
    env.execute()
  }

  def flat(): Unit = {
    val in: DataStream[String] = env.fromElements("now to rise", "and move on")
    val out = in.flatMap(new WordFlatMapFunction)
    // and move on now to rise
    out.print()
  }

  // map
  class F2CMapFunction extends MapFunction[SensorReading, SensorReading] {
    override def map(r: SensorReading): SensorReading = {
      val celsius = (r.temp - 32) * (5.0 / 9.0)
      SensorReading(r.id, r.ts, r.temp)
    }
  }

  // filter
  class AlertFilterFunction extends FilterFunction[SensorReading] {
    override def filter(r: SensorReading): Boolean = {
      r.temp > 50
    }
  }

  // flatMap
  class WordFlatMapFunction extends FlatMapFunction[String, String] {
    def flatMap(
        sentence: String,
        collector: Collector[
          String
        ] // flat input string into result string collection by collector
    ): Unit = {
      sentence.split(" ").foreach(collector.collect)
    }
  }
}
