package com.yinzige.sensor

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.scala.createTypeInformation
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.{
  DataStream,
  StreamExecutionEnvironment
}
import org.apache.flink.streaming.api.windowing.time.Time

// compute avg temperature every 5s for each sensor.
object AverageSensorReadings {
  def main(args: Array[String]): Unit = {
    // 1. set streaming execute env
    val env = StreamExecutionEnvironment.getExecutionEnvironment

    // use event time
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    // 2. read streams from DataSources
    val sensorDs: DataStream[SensorReading] = env
      .addSource(new SensorSource)
      .assignTimestampsAndWatermarks(new SensorTimeAssigner)

    // 3. stream transformation implement by app logic
    // convert SensorReading
    val avgTemps = sensorDs
      .map(r => {
        val c = (r.temp - 32) * (5.0 / 9.0) // 1. convert to celsius
        SensorReading(r.id, r.ts, c)
      })
      // .map(new F2CMapFunction)
      .keyBy(_.id) // 2. keyed by sensor id
      .timeWindow(Time.seconds(5)) // 3. reading 5s tumbling window
      .apply(
        new TempAvgWinFunction
      ) // 4. use user-defined window function, compute window result

    // 4. output result to sinks
    avgTemps.print()

    // 5. execute app
    env.execute("compute avg sensor temps")
  }
}
