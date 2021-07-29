package canal

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableResult}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * @program: FlinkSql
 * @description: ${description}
 * @author: yang
 * @create: 2021-06-03 16:31
 */
object KafkaToEs {
  def main(args: Array[String]): Unit = {
    //1、环境准备
    //hadoop101
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //   env.setStateBackend(new FsStateBackend("hdfs://123.207.27.238:8020/flink/mysql/checkpoints"))
    //  env.setStateBackend(new FsStateBackend("file:///D://tmp//flink/mysql"))
    env.setStateBackend(new FsStateBackend("file:///root/flink1.11/flink-1.11.3/job/checkpoint"))
    env.enableCheckpointing(1000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)))
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    //读取mysql表数据1
    val createMysqlUserTabel =
      """
        |CREATE TABLE `user` (
        |    id INT,
        |    name STRING,
        |    create_time TIMESTAMP(3)
        |) WITH (
        |'connector' = 'kafka-0.10',
        | 'topic' = 'user_changelog_json',
        | 'properties.bootstrap.servers' = '123.207.27.238:9092',
        | 'properties.group.id' = 'test_canal',
        | 'format' = 'changelog-json',
        | 'scan.startup.mode' = 'latest-offset'
        |)
    """.stripMargin


    //读取mysql表数据2
    val createMysqlWeblogTabel =
      """
        |CREATE TABLE user_info (
        |    id INT,
        |    username STRING,
        |    password STRING
        |) WITH (
        |'connector' = 'kafka-0.10',
        | 'topic' = 'user_info_changelog_json',
        | 'properties.bootstrap.servers' = '123.207.27.238:9092',
        | 'properties.group.id' = 'test_canal',
        | 'format' = 'changelog-json',
        | 'scan.startup.mode' = 'latest-offset'
        |)
    """.stripMargin

    //leftjoin 不支持kafka，只支持print
    val createMysqlSinkTabel =
      """
        |CREATE TABLE user_all (
        |    id INT,
        |    name STRING,
        |    create_time TIMESTAMP(3),
        |    bid INT,
        |    username STRING,
        |    password STRING,
        |    primary key(id) not ENFORCED
        |) WITH (
        |'connector' = 'kafka-0.10',
        | 'topic' = 'Demo14',
        | 'properties.bootstrap.servers' = '123.207.27.238:9092',
        | 'properties.group.id' = 'test_canal',
        | 'format' = 'changelog-json',
        | 'scan.startup.mode' = 'latest-offset'
        |)
    """.stripMargin

    val unoinSql =
      """
        |insert into user_all
        |    select u.id,u.name,u.create_time,ui.bid,ui.username,ui.password
        |    from (select id,name,create_time from `user`) as u
        |    left JOIN (select id as bid,username,password from user_info) as ui
        |    on u.id = ui.bid
      """.stripMargin

    tableEnv.executeSql(createMysqlUserTabel)
    tableEnv.executeSql(createMysqlWeblogTabel)
    tableEnv.executeSql(createMysqlSinkTabel)

    val result: TableResult = tableEnv.executeSql(unoinSql)
    result.print()

  }
}
