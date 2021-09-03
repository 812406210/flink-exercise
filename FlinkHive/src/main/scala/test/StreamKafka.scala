package test

import java.util.Properties

import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer

/**
 * @program: FlinkSql
 * @description: ${description}
 * @author: yang
 * @create: 2021-08-20 13:15
 */
object StreamKafka {

  def main(args: Array[String]): Unit = {
    val  env = StreamExecutionEnvironment.getExecutionEnvironment;
    //设置并行度,为了方便测试，查看消息的顺序，这里设置为1，可以更改为多并行度
    env.setParallelism(1);
    //checkpoint的设置
    //每隔10s进行启动一个检查点【设置checkpoint的周期】
    env.enableCheckpointing(30000);
    //设置模式为：exactly_one，仅一次语义
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
    //确保检查点之间有1s的时间间隔【checkpoint最小间隔】
//    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000);
//    //检查点必须在10s之内完成，或者被丢弃【checkpoint超时时间】
//    env.getCheckpointConfig.setCheckpointTimeout(10000);
//    //同一时间只允许进行一次检查点
//    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1);
//    //表示一旦Flink程序被cancel后，会保留checkpoint数据，以便根据实际需要恢复到指定的checkpoint
//    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
//    //设置statebackend,将检查点保存在hdfs上面，默认保存在内存中。这里先保存到本地
//    env.setStateBackend(new FsStateBackend("file:///D:/study_workspace/flink_demo/flink-java/StateBackEnd"));

    //设置kafka消费参数; kafka直接发送单个的字符串即可。
    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "uat-datacenter1:9092,uat-datacenter2:9092,uat-datacenter3:9092")
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.put("group.id", "stream-group")

    val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("user_behavior", new SimpleStringSchema(), props))
    val value: DataStream[String] = inputStream.map(data => {
      println("data")
      data
    })
    value.print("data====>");
    env.execute();
  }

}
