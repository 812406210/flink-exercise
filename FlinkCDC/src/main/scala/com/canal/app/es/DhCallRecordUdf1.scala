package com.canal.app.es

import java.io.InputStream
import java.util.concurrent.TimeUnit

import org.apache.flink.api.common.restartstrategy.RestartStrategies
import org.apache.flink.api.common.time.Time
import org.apache.flink.api.java.utils.ParameterTool
import org.apache.flink.runtime.state.filesystem.FsStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.environment.CheckpointConfig
import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala._
import org.apache.flink.table.api.{EnvironmentSettings, TableResult}
import org.apache.flink.table.functions.ScalarFunction

/**
 * @program: FlinkSql
 * @description: 执行eg: ./flink run  -c job.DhCallRecordUdf FlinkSqlTest-1.0-SNAPSHOT.jar -config_path ./test.properties
 * @author: yang
 * @create: 2021-04-22 16:26
 */
object DhCallRecordUdf1 {

  def main(args: Array[String]): Unit = {

    //参数准备
//    val parameters = ParameterTool.fromArgs(args)
//    val path = parameters.get("config_path")
    val stream: InputStream = getClass.getResourceAsStream("/test.properties")
    val parameterTool: ParameterTool = ParameterTool.fromPropertiesFile(stream)
    val mysqlTableHost: String = parameterTool.get("mysqlTableHost")
    val mysqlTableUserName: String = parameterTool.get("mysqlTableUserName")
    val mysqlTablePassword: String = parameterTool.get("mysqlTablePassword")
    val elasticHost: String = parameterTool.get("elasticHost")
    println("============================mysqlTableHost===========",mysqlTableHost)

    //1、环境准备
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setStateBackend(new FsStateBackend("file:///data/soft/flink-1.11.3/flink-1.11.3/checkpoint/DhCallRecordUdf"))
    env.enableCheckpointing(8000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)))
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    tableEnv.registerFunction("orgSplit",new OrgSplit1())
    //读取呼叫call_display_number
    // hadoop101  10.10.0.12
    val createTabelEtlDisplayNumber =
      s"""
        |CREATE TABLE call_display_number (
        |    id BIGINT,
        |    bind_status INT
        |) WITH (
        |    'connector' = 'mysql-cdc',
        |    'hostname' = '${mysqlTableHost}',
        |    'port' = '3306',
        |    'username' = '${mysqlTableUserName}',
        |    'password' = '${mysqlTablePassword}',
        |    'database-name' = 'callcenter',
        |    'table-name' = 'call_display_number'
        |)
    """.stripMargin

    //读取呼叫call_record
    val createTabelEtlCallRecord =
      s"""
        |CREATE TABLE call_record (
        |    id BIGINT,
        |    call_source VARCHAR,
        |    type VARCHAR,
        |    app_id VARCHAR,
        |    tenant_id BIGINT,
        |    tenant_name VARCHAR,
        |    tenant_line_id BIGINT,
        |    call_id VARCHAR,
        |    remote_queue_id VARCHAR,
        |    remote_queue_name VARCHAR,
        |    queue_id BIGINT,
        |    user_id BIGINT,
        |    consultant_id BIGINT,
        |    consultant_name VARCHAR,
        |    user_num VARCHAR,
        |    work_no VARCHAR,
        |    ext_num VARCHAR,
        |    `called` VARCHAR,
        |    caller VARCHAR,
        |    display_number_id BIGINT,
        |    display_number VARCHAR,
        |    display_number_source VARCHAR,
        |    remote_group_id BIGINT,
        |    display_number_group_id BIGINT,
        |    remote_group_name VARCHAR,
        |    area_id BIGINT,
        |    province_name VARCHAR,
        |    city_name VARCHAR,
        |    call_direct INT,
        |    ring_time TIMESTAMP,
        |    answer_time TIMESTAMP,
        |    start_time TIMESTAMP,
        |    end_time TIMESTAMP,
        |    ring_time_len INT,
        |    talk_time_len INT,
        |    total_time_len INT,
        |    drop_cause VARCHAR,
        |    telecom INT,
        |    hungup_flag INT,
        |    record_url VARCHAR,
        |    local_record_url VARCHAR,
        |    create_ts TIMESTAMP
        |) WITH (
        |    'connector' = 'mysql-cdc',
        |    'hostname' = '${mysqlTableHost}',
        |    'port' = '3306',
        |    'username' = '${mysqlTableUserName}',
        |    'password' = '${mysqlTablePassword}',
        |    'database-name' = 'callcenter',
        |    'table-name' = 'call_record'
        |)
    """.stripMargin


    //读取privilege_center库中的u_user
    val createTabelEtlUUser =
      s"""
        |CREATE TABLE u_user (
        |    id BIGINT,
        |    org_id BIGINT
        |) WITH (
        |    'connector' = 'mysql-cdc',
        |    'hostname' = '${mysqlTableHost}',
        |    'port' = '3306',
        |    'username' = '${mysqlTableUserName}',
        |    'password' = '${mysqlTablePassword}',
        |    'database-name' = 'privilege_center',
        |    'table-name' = 'u_user'
        |)
    """.stripMargin

    //读取privilege_center库中的u_organization
    val createTabelEtlUOrganization =
      s"""
        |CREATE TABLE u_organization (
        |    id BIGINT,
        |    org_name VARCHAR,
        |    org_level INT,
        |    parent_id BIGINT,
        |    org_full_id VARCHAR,
        |    org_full_name VARCHAR,
        |    defaulted INT,
        |    order_num INT
        |) WITH (
        |    'connector' = 'mysql-cdc',
        |    'hostname' = '${mysqlTableHost}',
        |    'port' = '3306',
        |    'username' = '${mysqlTableUserName}',
        |    'password' = '${mysqlTablePassword}',
        |    'database-name' = 'privilege_center',
        |    'table-name' = 'u_organization'
        |)
    """.stripMargin


    // sink数据至es
    val createSinkTabel =
    s"""
      |CREATE TABLE flinksql (
      |    id BIGINT,
      |    call_source VARCHAR,
      |    type VARCHAR,
      |    app_id VARCHAR,
      |    tenant_id BIGINT,
      |    tenant_name VARCHAR,
      |    tenant_line_id BIGINT,
      |    call_id VARCHAR,
      |    remote_queue_id VARCHAR,
      |    remote_queue_name VARCHAR,
      |    queue_id BIGINT,
      |    user_id BIGINT,
      |    consultant_id BIGINT,
      |    consultant_name VARCHAR,
      |    user_num VARCHAR,
      |    work_no VARCHAR,
      |    ext_num VARCHAR,
      |    `called` VARCHAR,
      |    caller VARCHAR,
      |    display_number_id BIGINT,
      |    display_number VARCHAR,
      |    display_number_source VARCHAR,
      |    remote_group_id BIGINT,
      |    display_number_group_id BIGINT,
      |    remote_group_name VARCHAR,
      |    area_id BIGINT,
      |    province_name VARCHAR,
      |    city_name VARCHAR,
      |    call_direct INT,
      |    ring_time TIMESTAMP,
      |    answer_time TIMESTAMP,
      |    start_time TIMESTAMP,
      |    end_time TIMESTAMP,
      |    ring_time_len INT,
      |    talk_time_len INT,
      |    total_time_len INT,
      |    drop_cause VARCHAR,
      |    telecom INT,
      |    hungup_flag INT,
      |    record_url VARCHAR,
      |    local_record_url VARCHAR,
      |    create_ts TIMESTAMP,
      |    bind_status INT,
      |    org_id BIGINT,
      |    org_name VARCHAR,
      |    org_level INT,
      |    parent_id BIGINT,
      |    org_full_id VARCHAR,
      |    org_full_name VARCHAR,
      |    defaulted INT,
      |    order_num INT,
      |    org1_id VARCHAR,
      |    org2_id VARCHAR,
      |    org3_id VARCHAR,
      |    org4_id VARCHAR,
      |    org5_id VARCHAR,
      |    org6_id VARCHAR,
      |    org1_name VARCHAR,
      |    org2_name VARCHAR,
      |    org3_name VARCHAR,
      |    org4_name VARCHAR,
      |    org5_name VARCHAR,
      |    org6_name VARCHAR,
      |    primary key(id) not ENFORCED
      |) WITH (
      |    'connector' = 'elasticsearch-6',
      |    'hosts' = '${elasticHost}',
      |    'index' = 'test_dh_callrecord_1',
      |    'document-type' = '_doc',
      |    'format'='json',
      |    'sink.bulk-flush.max-actions'='1'
      |)
    """.stripMargin

    //连表sql
    val leftJoinByIdSql =
    """
      |insert into flinksql
      |SELECT
      |    t1.*,
      |    t2.bind_status,
      |    t4.id as org_id,
      |    t4.org_name,
      |    t4.org_level,
      |    t4.parent_id,
      |    t4.org_full_id,
      |    t4.org_full_name,
      |    t4.defaulted,
      |    t4.order_num,
      |    orgSplit(t4.org_full_id)[1] as org1_id,
      |    orgSplit(t4.org_full_id)[2] as org2_id,
      |    orgSplit(t4.org_full_id)[3] as org3_id,
      |    orgSplit(t4.org_full_id)[4] as org4_id,
      |    orgSplit(t4.org_full_id)[5] as org5_id,
      |    orgSplit(t4.org_full_id)[6] as org6_id,
      |    orgSplit(t4.org_full_name)[1] as org1_name,
      |    orgSplit(t4.org_full_name)[2] as org2_name,
      |    orgSplit(t4.org_full_name)[3] as org3_name,
      |    orgSplit(t4.org_full_name)[4] as org4_name,
      |    orgSplit(t4.org_full_name)[5] as org5_name,
      |    orgSplit(t4.org_full_name)[6] as org6_name
      |FROM
      |	call_record as t1
      |	left join  call_display_number  as t2 ON t1.display_number_id = t2.id
      | left join  u_user as t3 on  t3.id = t1.user_id
      | left join  u_organization as t4 on t3.org_id = t4.id
      """.stripMargin

    //执行sql
    tableEnv.executeSql(createTabelEtlDisplayNumber)
    tableEnv.executeSql(createTabelEtlCallRecord)
    tableEnv.executeSql(createTabelEtlUUser)
    tableEnv.executeSql(createTabelEtlUOrganization)
    tableEnv.executeSql(createSinkTabel)
    val result: TableResult = tableEnv.executeSql(leftJoinByIdSql)
    result.print()
    tableEnv.execute("测试")

  }
}

class  OrgSplit1() extends ScalarFunction {

  var orgs: Array[String] = null
  def eval(org: String): Array[String] = {
    if(org  != null ) {
      orgs = org.split("-")
      orgs
    }else{
      new Array[String](0)
    }
  }
}







