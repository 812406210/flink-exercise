package test

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, TableResult}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * @program: demo
 * @description: 从kafka0.10读取数据，sink到es或者kafka
 * @author: yang
 * @create: 2021-01-15 11:48
 */
object SqlJoinMysqlTest {

  def main(args: Array[String]): Unit = {
    //1、环境准备
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //   env.setStateBackend(new FsStateBackend("hdfs://123.207.27.238:8020/flink/mysql/checkpoints"))
   // env.setStateBackend(new FsStateBackend("file:///D://tmp//flink/mysql"))
    env.setStateBackend(new FsStateBackend("file:///root/flink1.11/flink-1.11.3/job/checkpoint"))
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
        |    create_time TIMESTAMP(3),
        |    primary key (id) not ENFORCED
        |) WITH (
        |    'connector' = 'jdbc',
        |    'url' = 'jdbc:mysql://hadoop101:3306/test',
        |    'table-name' = 'user',
        |    'driver' = 'com.mysql.cj.jdbc.Driver',
        |    'username' = 'root',
        |    'password' = 'yang156122',
        |    'sink.buffer-flush.interval' = '1s'
        |)
    """.stripMargin

    //读取mysql表数据2
    val createMysqlWeblogTabel =
      """
        |CREATE TABLE user_info (
        |    id INT,
        |    username STRING,
        |    password STRING,
        |    primary key (id) not ENFORCED
        |) WITH (
        |    'connector' = 'jdbc',
        |    'url' = 'jdbc:mysql://hadoop101:3306/test',
        |    'table-name' = 'user_info',
        |    'driver' = 'com.mysql.cj.jdbc.Driver',
        |    'username' = 'root',
        |    'password' = 'yang156122',
        |    'sink.buffer-flush.interval' = '1s'
        |)
    """.stripMargin

    //写入mysql表数据
    val createMysqlSinkTabel =
      """
        |CREATE TABLE user_all (
        |    id INT,
        |    name STRING,
        |    create_time TIMESTAMP(3),
        |    bid INT,
        |    username STRING,
        |    password STRING,
        |    primary key (id) not ENFORCED
        |) WITH (
        |    'connector' = 'jdbc',
        |    'url' = 'jdbc:mysql://hadoop101:3306/test',
        |    'table-name' = 'user_all',
        |    'driver' = 'com.mysql.cj.jdbc.Driver',
        |    'username' = 'root',
        |    'password' = 'yang156122',
        |    'sink.buffer-flush.interval' = '1s'
        |)
    """.stripMargin

    val insertSql =
      """
        |insert into user_all
        |    select u.id,u.name,u.create_time,ui.bid,ui.username,ui.password
        |    from (select id,name,create_time from `user`) as u
        |    left JOIN (select id as bid,username,password from user_info) as ui
        |    on u.id = ui.bid
        |""".stripMargin

    tableEnv.executeSql(createMysqlUserTabel)
    tableEnv.executeSql(createMysqlWeblogTabel)
    tableEnv.executeSql(createMysqlSinkTabel)
    val result: TableResult = tableEnv.executeSql(insertSql)
    result.print()
  }
}
