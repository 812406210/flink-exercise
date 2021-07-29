package hive

import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment

/**
 * @program: FlinkSql
 * @description: ${description}
 * @author: yang
 * @create: 2021-06-18 15:23
 */
object mysql2hive {

  def main(args: Array[String]): Unit = {
    //1、环境准备
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    //   env.setStateBackend(new FsStateBackend("hdfs://uat-datacenter1:8020/flink/mysql/checkpoints"))
//    env.setStateBackend(new FsStateBackend("file:///D://tmp//flink/mysql"))
//    //env.setStateBackend(new FsStateBackend("file:///root/flink1.11/flink-1.11.3/job/checkpoint"))
//    env.enableCheckpointing(8000)
//    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
//    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
//    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)))
//    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

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
        |    'debezium.snapshot.locking.mode' = 'none',
        |    'server-time-zone' = 'Asia/Shanghai'
        |)
    """.stripMargin

    //changelog-json   debezium-json
    val createUser2Kafka =
      """
        |CREATE TABLE kafkaUser (
        |    id INT,
        |    name STRING,
        |    create_time TIMESTAMP(3),
        |    primary key(id) not ENFORCED
        |) WITH (
        |   'connector' = 'kafka',
        |   'topic' = 'kafka-test',
        |   'properties.group.id' = 'testGroup',
        |   'scan.startup.mode' = 'latest-offset',
        |   'properties.bootstrap.servers' = 'hadoop103:9092',
        |   'format' = 'debezium-json'
        |)
    """.stripMargin


    val kafkaUserSql =
      """
        |insert into kafkaUser
        |    select id,name,create_time
        |    from `user`
      """.stripMargin

    //user
    tableEnv.executeSql(createMysqlUserTabel)
    tableEnv.executeSql(createUser2Kafka)
    tableEnv.executeSql(kafkaUserSql)
  }

}
