package canal

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api.{EnvironmentSettings, TableResult}

/**
 * @program: demo
 * @description: 从kafka0.10读取数据，sink到es或者kafka
 * @author: yang
 * @create: 2021-01-15 11:48
 * @attention support  changelog-json
 */
object Mysql2Kafka {

  def main(args: Array[String]): Unit = {
    //1、环境准备
    //hadoop101
    val env = StreamExecutionEnvironment.getExecutionEnvironment
 //   env.setStateBackend(new FsStateBackend("hdfs://uat-datacenter1:8020/flink/mysql/checkpoints"))
    env.setStateBackend(new FsStateBackend("file:///D://tmp//flink/mysql"))
    //env.setStateBackend(new FsStateBackend("file:///root/flink1.11/flink-1.11.3/job/checkpoint"))
    env.enableCheckpointing(8000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)))
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)


    val createMysqlUserTabel =
      """
        |CREATE TABLE `user` (
        |    id INT,
        |    name STRING,
        |    create_time TIMESTAMP(3)
        |) WITH (
        |    'connector' = 'mysql-cdc',
        |    'hostname' = 'hadoop101',
        |    'port' = '3306',
        |    'username' = 'root',
        |    'password' = 'yang156122',
        |    'database-name' = 'test',
        |    'table-name' = 'user',
        |    'server-time-zone' = 'Asia/Shanghai'
        |)
    """.stripMargin

    //changelog-json
    val createUser2Kafka =
      """
        |CREATE TABLE kafkaUser (
        |    id INT,
        |    name STRING,
        |    create_time TIMESTAMP(3),
        |    primary key(id) not ENFORCED
        |) WITH (
        |   'connector' = 'kafka-0.10',
        |   'topic' = 'user_changelog_json',
        |   'scan.startup.mode' = 'latest-offset',
        |   'properties.bootstrap.servers' = 'uat-datacenter1:9092',
        |   'format' = 'changelog-json'
        |)
    """.stripMargin


    val kafkaUserSql =
      """
        |insert into kafkaUser
        |    select *
        |    from `user`
      """.stripMargin


    //读取mysql表数据2
//    'scan.startup.mode' = 'specific-offset',
//    'scan.startup.specific-offset.file' = 'mysql-bin.000026',
//    'scan.startup.specific-offset.pos' = '218950495'
    val createUserInfoTabel =
      """
        |CREATE TABLE user_info (
        |    id INT,
        |    username STRING,
        |    password STRING
        |) WITH (
        |    'connector' = 'mysql-cdc',
        |    'hostname' = 'hadoop101',
        |    'port' = '3306',
        |    'username' = 'root',
        |    'password' = 'yang156122',
        |    'database-name' = 'test',
        |    'table-name' = 'user_info',
        |    'server-time-zone' = 'Asia/Shanghai'
        |)
    """.stripMargin


    //'connector' = 'kafka-0.10',
    val createMysqlSinkTabel =
      """
        |CREATE TABLE kafkaUserInfo (
        |    id INT,
        |    username STRING,
        |    password STRING,
        |    primary key(id) not ENFORCED
        |) WITH (
        |   'connector' = 'kafka-0.10',
        |   'topic' = 'user_info_changelog_json',
        |   'scan.startup.mode' = 'latest-offset',
        |   'properties.bootstrap.servers' = 'uat-datacenter1:9092',
        |   'format' = 'changelog-json'
        |)
    """.stripMargin

    val  kafkaUserInfoSql =
      """
        |insert into kafkaUserInfo
        |   select * from user_info
      """.stripMargin

    //user
    tableEnv.executeSql(createMysqlUserTabel)
    tableEnv.executeSql(createUser2Kafka)
    //user_info
    tableEnv.executeSql(createUserInfoTabel)
    tableEnv.executeSql(createMysqlSinkTabel)

    tableEnv.executeSql(kafkaUserSql)
    tableEnv.executeSql(kafkaUserInfoSql)
    //result.print()
  }
}

