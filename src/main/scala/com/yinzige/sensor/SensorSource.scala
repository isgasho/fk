package com.yinzige.sensor

import java.util.Calendar
import org.apache.flink.streaming.api.functions.source.{
  RichParallelSourceFunction,
  SourceFunction
}
import scala.util.Random

class SensorSource extends RichParallelSourceFunction[SensorReading] {
  var running = true

  // SensorReading --continuously--> sourceContext
  override def run(
      sourceContext: SourceFunction.SourceContext[SensorReading]
  ): Unit = {
    val r = new Random()
    val taskIdx = this.getRuntimeContext.getIndexOfThisSubtask

    // init sensors with random temp
    var temps = (1 to 10).map(i =>
      ("sensor_" + (taskIdx * 10 + i), 65 + (r.nextGaussian() * 20))
    )

    // running until canceled
    while (running) {
      // update temp
      temps = temps.map(s => (s._1, s._2 + (r.nextGaussian() * 0.5)))

      // emit all sensor temps
      val curTs = Calendar.getInstance.getTimeInMillis
      temps.foreach(s => {
        sourceContext.collect(SensorReading(s._1, curTs, s._2))
      })

      Thread.sleep(100)
    }
  }

  override def cancel(): Unit = {
    // notify run() to stop
    running = false
  }
}
