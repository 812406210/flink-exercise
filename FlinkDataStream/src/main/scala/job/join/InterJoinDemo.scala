package job.join


import job.window.SensorReading
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
/**
 * @program: FlinkSql
 * @description: ${description}
 * @author: yang
 * @create: 2021-07-07 10:27
 */
object InterJoinDemo {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val inputFile:String = "G:\\Java\\Flink\\guigu\\flink\\src\\main\\resources\\sensor.txt"
    val input: DataStream[String] = env.readTextFile(inputFile)

    // 源1
    val dataStream1: DataStream[SensorReading] = input.map(data => {
      val arr: Array[String] = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    // 源2
    val dataStream2: DataStream[SensorReading] = input.map(data => {
      val arr: Array[String] = data.split(",")
      SensorReading(arr(0), arr(1).toLong, arr(2).toDouble)
    })

    dataStream1
      .keyBy(_.id)
      .intervalJoin(dataStream2.keyBy(_.id))
      .between(Time.milliseconds(-2),Time.milliseconds(2))
      .process(new ProcessJoinFunction[SensorReading,SensorReading,Seq[SensorReading]] {

        override def processElement(in1: SensorReading, in2: SensorReading, context: ProcessJoinFunction[SensorReading, SensorReading, Seq[SensorReading]]#Context, collector: Collector[Seq[SensorReading]]): Unit = {
          //TODO

        }
      })

  }
}
