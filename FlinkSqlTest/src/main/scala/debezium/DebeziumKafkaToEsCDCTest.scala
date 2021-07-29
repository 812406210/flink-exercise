package debezium

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableResult}
import org.apache.flink.table.functions.ScalarFunction

/**
 * @program: demo
 * @description: 从kafka0.10读取数据，sink到es或者kafka
 * @author: yang
 * @create: 2021-01-15 11:48
 */
object DebeziumKafkaToEsCDCTest {

  def main(args: Array[String]): Unit = {
    //1、环境准备
    //hadoop101
    val env = StreamExecutionEnvironment.getExecutionEnvironment
 //   env.setStateBackend(new FsStateBackend("hdfs://uat-datacenter1:8020/flink/mysql/checkpoints"))
  //  env.setStateBackend(new FsStateBackend("file:///D://tmp//flink/mysql"))
    env.setStateBackend(new FsStateBackend("file:///root/flink1.11/flink-1.11.3/job/checkpoint"))
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)))
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    tableEnv.registerFunction("utc2local",new UTC2Local())
    //读取kafka表数据1
    val createKafkaUserTabel =
      """
        |CREATE TABLE `user_all_kafka` (
        |    id INT,
        |    name STRING,
        |    create_time TIMESTAMP(3),
        |    etl_time TIMESTAMP(3),
        |    bid INT,
        |    username STRING,
        |    password STRING,
        |    primary key(id) not ENFORCED
        |) WITH (
        |'connector' = 'kafka-0.10',
        | 'topic' = 'user_all_debezium',
        | 'properties.bootstrap.servers' = 'uat-datacenter1:9092',
        | 'properties.group.id' = 'test_debezium',
        | 'format' = 'changelog-json' ,
        | 'scan.startup.mode' = 'latest-offset'
        |)
    """.stripMargin

    val createEsSinkTabel =
      """
        |CREATE TABLE user_all_es_etl (
        |    id INT,
        |    name STRING,
        |    create_time TIMESTAMP(3),
        |    etl_time TIMESTAMP(3),
        |    bid INT,
        |    username STRING,
        |    password STRING,
        |    primary key(id) not ENFORCED
        |) WITH (
        |    'connector' = 'elasticsearch-6',
        |    'hosts' = 'http://uat-datacenter1:9200',
        |    'index' = 'user_all_debezium',
        |    'document-type' = '_doc',
        |    'format'='changelog-json',
        |    'sink.bulk-flush.max-actions'='1'
        |)
    """.stripMargin

    val insertSql =
      """
        |insert into user_all_es_etl
        | select id,name,create_time,utc2local(etl_time) as `etl_time`,bid,username,password from  user_all_kafka
      """.stripMargin

    tableEnv.executeSql(createKafkaUserTabel)
    tableEnv.executeSql(createEsSinkTabel)

    val result: TableResult = tableEnv.executeSql(insertSql)
    result.print()

  }
}
class  UTC2Local() extends ScalarFunction {
  //必须叫 eval
  def eval(s: Timestamp): Timestamp = {
    var newTime = new Timestamp(s.getTime + 28800000)
    val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val timestampFormat: Timestamp = Timestamp.valueOf(format.format(newTime))
    timestampFormat
  }
}
