package com.canal.doris

import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.util.Properties
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
 * @program: demo
 * @description: 从kafka0.10读取数据，sink到es或者kafka
 * @author: yang
 * @create: 2021-01-15 11:48
 *          错误：Exception in thread "main" org.apache.flink.table.api.TableException: Table sink 'default_catalog.default_database.user_all_doris_etl'
 *          doesn't support consuming update and delete changes which is produced by node TableSourceScan(table=[[default_catalog, default_database, user_all_kafka]],
 *          fields=[id, name, create_time, etl_time, bid, username, password])
 *
 *
 *
 *          ## 查看分区
 *          SHOW PARTITIONS FROM user_all_doris_etl;
 *          #创建分区表
 *          CREATE TABLE user_all_doris_etl
 *          (
 *          id INTEGER,
 *          create_time date,
 *          name VARCHAR(256) DEFAULT '',
 *          etl_time DATETIME,
 *          bid INTEGER,
 *          username VARCHAR(256) DEFAULT '',
 *          password VARCHAR(256) DEFAULT ''
 *
 *          )
 *          ENGINE=olap
 *          DUPLICATE KEY(id, create_time)
 *          PARTITION BY RANGE (create_time) (
 *          PARTITION p20210601 VALUES LESS THAN ("2021-06-01"),
 *          PARTITION p20210602 VALUES LESS THAN ("2021-06-02"),
 *          PARTITION p20210603 VALUES LESS THAN ("2021-06-03"),
 *          PARTITION p20210604 VALUES LESS THAN ("2021-06-04"),
 *          PARTITION p20210605 VALUES LESS THAN ("2021-06-05"),
 *          PARTITION p20210606 VALUES LESS THAN ("2021-06-06"),
 *          PARTITION p20210607 VALUES LESS THAN ("2021-06-07"),
 *          PARTITION p20210608 VALUES LESS THAN ("2021-06-08"),
 *          PARTITION p20210609 VALUES LESS THAN ("2021-06-09")
 *          )
 *          DISTRIBUTED BY HASH(id) BUCKETS 3
 *          PROPERTIES(
 *          "replication_num" = "3",
 *          "dynamic_partition.enable" = "true",
 *          "dynamic_partition.time_unit" = "DAY",
 *          "dynamic_partition.start" = "-3",
 *          "dynamic_partition.end" = "3",
 *          "dynamic_partition.prefix" = "p",
 *          "dynamic_partition.buckets" = "32"
 *          );
 */
object KafkaToDoris {

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
    env.setParallelism(1)
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)

//    val props = new Properties()
//    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "192.168.75.101:9092")
//    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest-offset")
//    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
//    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
//    val input: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("kafka-test", new SimpleStringSchema(), props))

//    tableEnv.registerFunction("utc2local",new UTC2Local())
    //读取kafka表数据1
    val createKafkaUserTabel =
      """
        |CREATE TABLE user_all_kafka (
        |    id INT,
        |    name STRING,
        |    create_time TIMESTAMP(3),
        |    etl_time TIMESTAMP(3),
        |    bid INT,
        |    username STRING,
        |    password STRING,
        |    primary key(id) not ENFORCED
        |) WITH (
        | 'connector' = 'kafka',
        | 'topic' = 'Demo14',
        | 'properties.bootstrap.servers' = '123.207.27.238:9092',
        | 'properties.group.id' = 'test_debezium',
        | 'format' = 'json' ,
        | 'scan.startup.mode' = 'latest-offset'
        |)
    """.stripMargin

    val createDorisSinkTabel =
      """
        |CREATE TABLE user_all_doris_etl (
        |    id INT,
        |    name STRING,
        |    create_time TIMESTAMP(3),
        |    etl_time TIMESTAMP(3),
        |    bid INT,
        |    username STRING,
        |    password STRING,
        |    primary key(id) not ENFORCED
        |) WITH (
        |    'connector' = 'doris',
        |    'jdbc-url' = 'jdbc:mysql://hadoop101:9030?doris',
        |    'load-url' = 'hadoop101:8030',
        |    'database-name' = 'doris',
        |    'table-name'='user_all_doris_etl',
        |    'username' = 'root',
        |    'password' = '',
        |    'sink.buffer-flush.max-rows' = '1',
        |    'sink.buffer-flush.max-bytes' = '4',
        |    'sink.buffer-flush.interval-ms' = '50',
        |    'sink.max-retries' = '3'
        |)
    """.stripMargin

    val insertSql =
      """
        |insert into user_all_doris_etl
        | select id,name,create_time, etl_time,bid,username,password from  user_all_kafka
      """.stripMargin

    tableEnv.executeSql(createKafkaUserTabel)
    tableEnv.executeSql(createDorisSinkTabel)

    val result: TableResult = tableEnv.executeSql(insertSql)
    result.print()

  }
}
//class  UTC2Local() extends ScalarFunction {
//  //必须叫 eval
//  def eval(s: Timestamp): Timestamp = {
//    var newTime = new Timestamp(s.getTime + 28800000)
//    val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
//    val timestampFormat: Timestamp = Timestamp.valueOf(format.format(newTime))
//    timestampFormat
//  }
//}
