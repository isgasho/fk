package com.yinzige.sensor

import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.windowing.time.Time

// extract timestamp from Record
// maxOutOfOrderness is 5s
class SensorTimeAssigner
    extends BoundedOutOfOrdernessTimestampExtractor[SensorReading](
      Time.seconds(5)
    ) {

  override def extractTimestamp(t: SensorReading): Long = {
    t.ts
  }
}
