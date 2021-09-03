package test

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.{EnvironmentSettings, TableResult}

/**
 * @program: demo
 * @description: 从kafka0.10读取数据，sink到es或者kafka
 * @author: yang
 * @create: 2021-01-15 11:48
 */
object SqlJoinMysql2EsCDCTest {

  def main(args: Array[String]): Unit = {
    //1、环境准备
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(new FsStateBackend("hdfs://hadoop101:8020/flink/es/checkpoints"))
    //env.setStateBackend(new FsStateBackend("file:///D://tmp//flink/es"))
//    env.setStateBackend(new FsStateBackend("file:///root/flink1.11/flink-1.11.3/job/es/checkpoint"))
    env.enableCheckpointing(50000)

    // 两次 checkpoint 的间隔时间至少为 1 s，默认是 0，立即进行下一次 checkpoint
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(1000);
    // checkpoint 语义设置为 EXACTLY_ONCE，这是默认语义
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    // 同一时间只能允许有一个 checkpoint
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(1)
    // 失败重启策略
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)))
    // 当 Flink 任务取消时，保留外部保存的 checkpoint 信息
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)
    // 当有较新的 Savepoint 时，作业也会从 Checkpoint 处恢复
    env.getCheckpointConfig.setPreferCheckpointForRecovery(true);
    // 允许实验性的功能：非对齐的 checkpoint，以提升性能
    env.getCheckpointConfig.enableUnalignedCheckpoints();
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    //读取mysql表数据1
    val createMysqlUserTabel =
      """
        |CREATE TABLE `user` (
        |    id INT,
        |    name STRING,
        |    age DOUBLE,
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
        |    age DOUBLE,
        |    create_time TIMESTAMP(3),
        |    bid INT,
        |    username STRING,
        |    password STRING,
        |    primary key(id) not ENFORCED
        |) WITH (
        |    'connector' = 'elasticsearch-6',
        |    'hosts' = 'http://123.207.27.238:9200',
        |    'index' = 'user_all',
        |    'document-type' = '_doc',
        |    'format'='json',
        |    'sink.bulk-flush.max-actions'='1'
        |)
    """.stripMargin

    val unoinSql =
      """
        |insert into user_all
        |    select u.id,u.name,u.age,u.create_time,ui.bid,ui.username,ui.password
        |    from (select id,name,age,create_time from `user`) as u
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
