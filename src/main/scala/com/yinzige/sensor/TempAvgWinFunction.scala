package com.yinzige.sensor

import org.apache.flink.streaming.api.scala.function.WindowFunction
import org.apache.flink.streaming.api.windowing.windows.TimeWindow
import org.apache.flink.util.Collector

// user defined WindowFunction to compute their logic for every Window records.
// this case compute average temp for these SensorReadings.
class TempAvgWinFunction
    extends WindowFunction[SensorReading, SensorReading, String, TimeWindow] {
  override def apply(
      key: String, // sensorId
      window: TimeWindow, // ts: [start, end]
      input: Iterable[SensorReading], // records
      out: Collector[SensorReading]
  ): Unit = {
    val (count, sum) = input.foldLeft(0, 0.0)((c, r) => (c._1, c._2 + r.temp))
    val avg = sum / count

    // emit avg result
    out.collect(SensorReading(key, window.getEnd, avg))
  }
}
