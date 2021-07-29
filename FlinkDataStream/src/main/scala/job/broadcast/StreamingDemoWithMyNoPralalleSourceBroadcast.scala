package job.broadcast

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import java.net.URL
import java.util

import org.apache.flink.api.common.state.MapStateDescriptor
import org.apache.flink.api.common.typeinfo.BasicTypeInfo
import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.datastream.BroadcastStream
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
/**
 * @program: FlinkSql
 * @description: ${description}
 * @author: yang
 * @create: 2021-07-19 16:47
 */
object StreamingDemoWithMyNoPralalleSourceBroadcast {

  def main(args: Array[String]): Unit = {
    //获取Flink的运行环境
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(4)
    val wordsConfig = new MapStateDescriptor(
      "wordsConfig",
      BasicTypeInfo.STRING_TYPE_INFO,
      BasicTypeInfo.STRING_TYPE_INFO)
    //获取数据源
    val broadcastStream: BroadcastStream[Long] = env.addSource(new MyNoParalleSource()).setParallelism(1).broadcast(wordsConfig)

    println(broadcastStream)
    env.execute("广播测试>>")
  }



}

class MyNoParalleSource extends SourceFunction[Long] {
  private var count = 1L
  private var isRunning = true

  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while(isRunning){
      ctx.collect(count);
      count = count +1
      //每秒产生一条数据
      Thread.sleep(1000);
    }
  }

  override def cancel(): Unit =  {isRunning = false}
}
