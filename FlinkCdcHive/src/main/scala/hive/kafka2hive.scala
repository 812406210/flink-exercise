package hive


import java.time.Duration

import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.api.environment.{CheckpointConfig, ExecutionCheckpointingOptions}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.{EnvironmentSettings, SqlDialect}
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.log4j.Level
import org.slf4j.LoggerFactory

/**
 * @program: FlinkSql
 * @description: ${description}
 * @author: yang
 * @create: 2021-06-18 19:19
 */
object kafka2hive {

  private var logger: org.slf4j.Logger = _
  def main(args: Array[String]): Unit = {
    logger = LoggerFactory.getLogger(this.getClass.getSimpleName)
    org.apache.log4j.Logger.getLogger("org.apache.hadoop").setLevel(Level.WARN)
    org.apache.log4j.Logger.getLogger("org.apache").setLevel(Level.INFO)

    // 初始化 stream 环境
    // 本地测试，需要 flink-runtime-web 依赖
    val env = StreamExecutionEnvironment.createLocalEnvironmentWithWebUI()
    //    val env = StreamExecutionEnvironment.getExecutionEnvironment

    //    参数读取
    //    val params = ParameterTool.fromArgs(args)
    //    env.getConfig.setGlobalJobParameters(params)

    // 失败重启,固定间隔,每隔3秒重启1次,总尝试重启10次
    //    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 3))
    // 本地测试线程 1
    env.setParallelism(1)

    // 事件处理的时间，由系统时间决定
    env.setStreamTimeCharacteristic(TimeCharacteristic.ProcessingTime)

    // 创建 streamTable 环境
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env, settings)

    // checkpoint 设置
    val tableConfig = tableEnv.getConfig.getConfiguration
    tableConfig.set(ExecutionCheckpointingOptions.CHECKPOINTING_MODE, CheckpointingMode.EXACTLY_ONCE)
    // checkpoint的超时时间周期，1 分钟做一次checkpoint
    tableConfig.set(ExecutionCheckpointingOptions.CHECKPOINTING_INTERVAL, Duration.ofSeconds(30))
    // checkpoint的超时时间, 检查点一分钟内没有完成将被丢弃
    //    tableConfig.set(ExecutionCheckpointingOptions.CHECKPOINTING_TIMEOUT, Duration.ofSeconds(60))
    // checkpoint 最小间隔，两个检查点之间至少间隔 30 秒
    //    tableConfig.set(ExecutionCheckpointingOptions.MIN_PAUSE_BETWEEN_CHECKPOINTS, Duration.ofSeconds(30))
    // 同一时间只允许进行一个检查点
    //    tableConfig.set(ExecutionCheckpointingOptions.MAX_CONCURRENT_CHECKPOINTS, Integer.valueOf(1))
    // 手动cancel时是否保留checkpoint
    //    tableConfig.set(ExecutionCheckpointingOptions.EXTERNALIZED_CHECKPOINT,
    //      CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    // 设置状态的最小空闲时间和最大的空闲时间
    //    tableEnv.getConfig.setIdleStateRetentionTime(Time.hours(12), Time.hours(24))

    // 加载配置
    val catalog_name = "user"
    val database = "default"
    val schemaDataBase = "myhive"
    val HIVE_CONF_DIR = "E:\\Flink\\FlinkSql\\FlinkCdcHive\\src\\main\\resources\\conf"

    // 构造 hive catalog
    val hiveCatalog = new HiveCatalog(
      catalog_name,
      database,
      HIVE_CONF_DIR
    )

    tableEnv.registerCatalog(catalog_name, hiveCatalog)
    tableEnv.useCatalog(catalog_name)

    // 构造 kafka source, 用 DEFAULT
    tableEnv.getConfig.setSqlDialect(SqlDialect.DEFAULT)
    tableEnv.useDatabase(schemaDataBase)

    // 如果catalog中 kafka source 已建表，先删除
    tableEnv.executeSql("drop table if exists kafkaSourceTable")
    // kafka source table
    val kafkaSourceDDL =
      """
        |create table kafka_source_Table (
        | user_name string,
        | user_id bigint,
        | `time` bigint,
        | ts as to_timestamp(from_unixtime(`time` / 1000, 'yyyy-MM-dd HH:mm:ss')),
        | rt as proctime()
        |) with (
        | 'connector' = 'kafka',
        | 'topic' = 'flink_test',
        | 'properties.bootstrap.servers' = '192.168.100.39:9092',
        | 'properties.group.id' = 'flink-test-group',
        | 'format' = 'json',
        | 'scan.startup.mode' = 'group-offsets'
        |)
      """.stripMargin
    tableEnv.executeSql(kafkaSourceDDL)

    // hive catalog
    tableEnv.getConfig.setSqlDialect(SqlDialect.HIVE)
    tableEnv.useDatabase(database)
    // 删除表，已有 hive 表的情况下不用删除和建表
    // tableEnv.executeSql("drop table if exists test_hive_table")
    // hive sink table
    // 每次 checkpoint 完成后才会真正写入 Hive, 生成一个文件
    val hiveSinkDDL =
    """
      |create table hive_sink_table (
      | user_name string,
      | user_id bigint,
      | `time` bigint,
      | `date` string
      |)
      |partitioned by (`date` string)
      |row format delimited fields terminated by '\t'
      |stored as orc
      |location 'hdfs://BigdataCluster/user/hive/warehouse/test_data.db/test/test_hive_table'
      |tblproperties (
      | 'orc.compress'='SNAPPY',
      | 'partition.time-extractor.timestamp-pattern' = '$date 00:00:00',
      | 'sink.partition-commit.trigger' = 'process-time',
      | 'sink.partition-commit.policy.kind' = 'metastore,success-file'
      |)
    """.stripMargin
    tableEnv.executeSql(hiveSinkDDL)
    // 'sink.partition-commit.delay' = '10 s',

    // 写数据
    val insertSql =
      """
        |insert into hive_sink_table
        |select
        | user_name,
        | user_id,
        | `time`,
        | date_format(ts, 'yyyy-MM-dd') as `date`
        |from schema_data.kafka_source_Table
      """.stripMargin
    tableEnv.executeSql(insertSql)


    // 测试打印输入数据, 1. 以 connector=print 打印; 2. 转成 dataStream.print
    //    val querySql =
    //      """
    //        |select
    //        | user_name,
    //        | user_id,
    //        | date_format(ts, 'yyyy-MM-dd') as `date`,
    //        | date_format(ts, 'HH'),
    //        | date_format(ts, 'mm')
    //        |from kafkaSourceTable
    //      """.stripMargin
    //    val table = tableEnv.sqlQuery(querySql)
    //    table.printSchema()
    //
    //    val resultDStream = tableEnv.toAppendStream[Row](table)
    //    resultDStream.print()
    //    env.execute("flink sql kafka 2 hive")

  }
}
