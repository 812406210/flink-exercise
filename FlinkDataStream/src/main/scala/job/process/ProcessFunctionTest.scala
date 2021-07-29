package job.process

import job.window.SensorReading
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.KeyedProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
/**
 * @program: FlinkSql
 * @description: ${description}
 * @author: yang
 * @create: 2021-07-26 17:13
 */
object ProcessFunctionTest {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)

    // 读取数据
    val inputStream = env.socketTextStream("hadoop103", 7777)

    // 先转换成样例类类型（简单转换操作）
    val dataStream = inputStream
      .map( data => {
        println(data)
        val arr = data.split(",")
        SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
      } )
    //      .keyBy(_.id)
    //      .process( new MyKeyedProcessFunction )

    val warningStream = dataStream
      .keyBy(_.id)
      .process( new TempIncreWarning(10000L) )

    warningStream.print()

    env.execute("process function test")


  }
}


class TempIncreWarning(interval:Long) extends KeyedProcessFunction[String,SensorReading,String] {

  lazy val lastTempState:ValueState[Double] = getRuntimeContext.getState(new ValueStateDescriptor[Double]("last-temp",classOf[Double]))

  lazy val timerTsState:ValueState[Long] = getRuntimeContext.getState(new ValueStateDescriptor[Long]("timer-ts",classOf[Long]))

  override def processElement(value: SensorReading, ctx: KeyedProcessFunction[String, SensorReading, String]#Context, out: Collector[String]): Unit = {
    //1、先取出开始状态
    val lastTemp: Double = lastTempState.value() // 0.0
    val timerTs: Long = timerTsState.value()  // 0

    //2、更新温度值
    lastTempState.update(value.temperature)
    if(value.temperature > lastTemp && timerTs == 0){
      // 如果温度上升，且没有定时器，那么注册当前时间10s之后的定时器,同时更新时间状态
      println(ctx.timerService().currentProcessingTime()) //1627293546510
      val ts = ctx.timerService().currentProcessingTime() + interval
      ctx.timerService().registerProcessingTimeTimer(ts)
      timerTsState.update(ts)
    }else if(value.temperature < lastTemp){
      println("删除定时器..........")
      //温度下降，删除定时器
      ctx.timerService().deleteProcessingTimeTimer(timerTs)
      timerTsState.clear()
    }

  }

  //定时器
  override def onTimer(timestamp: Long, ctx: KeyedProcessFunction[String, SensorReading, String]#OnTimerContext, out: Collector[String]): Unit = {
      out.collect("传感器" + ctx.getCurrentKey + "的温度连续" + interval/1000 + "秒连续上升")
      timerTsState.clear()
  }
}
