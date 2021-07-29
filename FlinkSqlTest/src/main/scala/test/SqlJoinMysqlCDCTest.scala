package test

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, TableResult}
import org.apache.flink.table.api.bridge.scala._

/**
 * @program: demo
 * @description: 从kafka0.10读取数据，sink到es或者kafka
 * @author: yang
 * @create: 2021-01-15 11:48
 */
object SqlJoinMysqlCDCTest {

  def main(args: Array[String]): Unit = {
    //1、环境准备
    //hadoop101
    val env = StreamExecutionEnvironment.getExecutionEnvironment
 //   env.setStateBackend(new FsStateBackend("hdfs://123.207.27.238:8020/flink/mysql/checkpoints"))
    env.setStateBackend(new FsStateBackend("file:///D://tmp//flink/mysql"))
    //env.setStateBackend(new FsStateBackend("file:///root/flink1.11/flink-1.11.3/job/checkpoint"))
    env.enableCheckpointing(8000)
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
        |    'connector' = 'mysql-cdc',
        |    'hostname' = 'hadoop101',
        |    'port' = '3306',
        |    'username' = 'root',
        |    'password' = 'yang156122',
        |    'database-name' = 'test',
        |    'table-name' = 'user'
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
        |    'connector' = 'mysql-cdc',
        |    'hostname' = 'hadoop101',
        |    'port' = '3306',
        |    'username' = 'root',
        |    'password' = 'yang156122',
        |    'database-name' = 'test',
        |    'table-name' = 'user_info'
        |)
    """.stripMargin

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
        |    'connector' = 'jdbc',
        |    'url' = 'jdbc:mysql://hadoop101:3306/test?characterEncoding=utf8&useSSL=false&serverTimezone=Asia/Shanghai',
        |    'driver' = 'com.mysql.jdbc.Driver',
        |    'table-name' = 'user_all',
        |    'username' = 'root',
        |    'password' = 'yang156122',
        |    'sink.buffer-flush.max-rows' = '2',
        |    'sink.buffer-flush.interval' = '1s'
        |)
    """.stripMargin

    val unoinSql =
      """
        |insert into user_all
        |    select u.id,u.name,u.create_time,ui.bid,ui.username,ui.password
        |    from `user` as u
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
