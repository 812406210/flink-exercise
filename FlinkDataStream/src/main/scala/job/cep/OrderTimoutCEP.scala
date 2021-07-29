package job.cep

import java.net.URL
import java.util

import org.apache.flink.cep.scala.pattern.Pattern
import org.apache.flink.cep.scala.{CEP, PatternStream}
import org.apache.flink.cep.{PatternSelectFunction, PatternTimeoutFunction}
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time

/**
 * @program: FlinkSql
 * @description: ${description}
 * @author: yang
 * @create: 2021-05-25 13:58
 */
// 定义输入输出样例类类型
case class OrderEvent(orderId: Long, eventType: String, txId: String, timestamp: Long)
case class OrderResult(orderId: Long, resultMsg: String)
object OrderTimoutCEP {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)

    //1、读取外部数据
    val receiptPath: URL = getClass.getResource("/OrderLog.csv")
    val receiptStream: DataStream[String] = env.readTextFile(receiptPath.getPath)
    val orderEventStream: KeyedStream[OrderEvent, Long] = receiptStream.map(data => {
      val datas: Array[String] = data.split(",")
      OrderEvent(datas(0).toLong, datas(1), datas(2), datas(3).toLong)
    }).assignAscendingTimestamps(_.timestamp * 1000).keyBy(_.orderId)

    //2、定义pattern
    val pattern: Pattern[OrderEvent, OrderEvent] = Pattern.begin[OrderEvent]("created").where(_.eventType == "create")
      .followedBy("payed").where(_.eventType == "pay")
      .within(Time.minutes(19))

    //3、将pattern放入流中
    val patternStream: PatternStream[OrderEvent] = CEP.pattern(orderEventStream, pattern)

    //4、从pattern提取对应的事件
    val orderTimeoutOutputTag  = new OutputTag[OrderResult]("timeout")
    val resultStream: DataStream[OrderResult] = patternStream.select(orderTimeoutOutputTag, new OrderTimeoutSelect(), new OrderPaySelect())

    resultStream.getSideOutput(orderTimeoutOutputTag).print("timeout")
    resultStream.print("payed")

    env.execute(" cep job")

  }

}

//超时输出
class OrderTimeoutSelect() extends PatternTimeoutFunction[OrderEvent,OrderResult] {
  override def timeout(pattern: util.Map[String, util.List[OrderEvent]], l: Long): OrderResult = {
    val timeoutOrderId : Long = pattern.get("created").iterator().next().orderId
    OrderResult(timeoutOrderId,"timeout pay")
  }
}

//正常输出
class OrderPaySelect() extends PatternSelectFunction[OrderEvent,OrderResult]{
  override def select(pattern: util.Map[String, util.List[OrderEvent]]): OrderResult = {
    val normalOrderId: Long = pattern.get("payed").iterator().next().orderId
    OrderResult(normalOrderId,"payed successfully")
  }
}