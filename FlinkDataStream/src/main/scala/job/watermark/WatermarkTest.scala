package job.watermark

import job.window.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.functions.timestamps.BoundedOutOfOrdernessTimestampExtractor
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector

/**
 * @program: FlinkSql
 * @description: ${description}
 * @author: yang
 * @create: 2021-07-27 10:57
 */
object WatermarkTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取数据
    val inputStream = env.socketTextStream("hadoop103", 7777)

    // 先转换成样例类类型（简单转换操作）
    val dataStream = inputStream
      .map( data => {
        //println(data)
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)

      } ).assignTimestampsAndWatermarks(new BoundedOutOfOrdernessTimestampExtractor[SensorReading](Time.seconds(5)) {
      override def extractTimestamp(element: SensorReading): Long = element.timestamp * 1000L
    })

    val warningStream = dataStream
      .keyBy(_.id)
      .min(2)

    warningStream.print()

    env.execute("process function test")


  }
}

class MyKeyedProcessFunction extends KeyedProcessFunction[String,SensorReading,String]{

  lazy val  myState:ValueState[Int] = getRuntimeContext.getState(new  ValueStateDescriptor[Int]("mystate",classOf[Int]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    ctx.getCurrentKey
    ctx.timestamp()
    ctx.timerService().currentWatermark()
    ctx.timerService().registerEventTimeTimer(ctx.timestamp() + 60000L)
  }

  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {

  }
}
