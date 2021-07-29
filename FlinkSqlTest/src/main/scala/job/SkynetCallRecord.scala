package job

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
 * @program: FlinkSql
 * @description: 机会，呼叫
 * @author: yang
 * @create: 2021-04-22 11:11
 */
object SkynetCallRecord {
  def main(args: Array[String]): Unit = {
    //1、环境准备
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    // env.setStateBackend(new FsStateBackend("hdfs://uat-datacenter1:8020/flink/es/checkpoints"))
    //env.setStateBackend(new FsStateBackend("file:///D://tmp//flink/es"))
    env.setStateBackend(new FsStateBackend("file:///root/flink1.11/flink-1.11.3/job/es/checkpoint"))
    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3, Time.of(10, TimeUnit.SECONDS)))
    env.getCheckpointConfig.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION)

    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    val tableEnv = StreamTableEnvironment.create(env, settings)
    //读取天网t_opportunity表数据1
    val createTabelTOpportunity =
      """
        |CREATE TABLE t_opportunity (
        |    id BIGINT,
        |    user_id BIGINT,
        |    user_account VARCHAR,
        |    student_id BIGINT,
        |    province_id BIGINT,
        |    city_id BIGINT,
        |    create_user_account VARCHAR,
        |    create_user_id BIGINT,
        |    create_time TIMESTAMP,
        |    allocate_time TIMESTAMP,
        |    apply_time TIMESTAMP,
        |    friend_time TIMESTAMP,
        |    site_id VARCHAR,
        |    site_source VARCHAR,
        |    intention VARCHAR,
        |    state VARCHAR,
        |    first_proj_id BIGINT,
        |    oppor_source VARCHAR,
        |    update_user_id BIGINT,
        |    update_time TIMESTAMP,
        |    first_valid_follow_time TIMESTAMP,
        |    valid_follow_time TIMESTAMP,
        |    valid_follow_counts INT,
        |    follow_time TIMESTAMP,
        |    abandon_status INT,
        |    abandon_time TIMESTAMP,
        |    silence_status INT,
        |    hidden_status INT,
        |    second_consult_status INT,
        |    advertiser_code VARCHAR,
        |    advertiser VARCHAR,
        |    get_status INT,
        |    get_time TIMESTAMP,
        |    delete_flag INT,
        |    org_id1 BIGINT,
        |    org_id2 BIGINT,
        |    org_id3 BIGINT,
        |    org_id4 BIGINT,
        |    org_id5 BIGINT,
        |    tenant_id BIGINT,
        |    opportunity_code VARCHAR,
        |    last_follow_consultant_id BIGINT
        |) WITH (
        |    'connector' = 'mysql-cdc',
        |    'hostname' = 'hadoop101',
        |    'port' = '3306',
        |    'username' = 'root',
        |    'password' = 'yang156122',
        |    'database-name' = 'skynet',
        |    'table-name' = 't_opportunity'
        |)
    """.stripMargin


    //读取天网t_call_record表数据2
    val createTabelTCallRecord =
      """
        |CREATE TABLE t_call_record (
        |    id INT,
        |    opportunity_id BIGINT,
        |    call_time TIMESTAMP,
        |    record_status INT,
        |    create_time TIMESTAMP,
        |    mobile VARCHAR,
        |    call_id VARCHAR,
        |    call_duration INT,
        |    stage INT,
        |    type VARCHAR
        |) WITH (
        |    'connector' = 'mysql-cdc',
        |    'hostname' = 'hadoop101',
        |    'port' = '3306',
        |    'username' = 'root',
        |    'password' = 'yang156122',
        |    'database-name' = 'skynet',
        |    'table-name' = 't_call_record'
        |)
    """.stripMargin


    //读取呼叫call_record表数据3
    val createTabelCallRecord =
      """
        |CREATE TABLE call_record (
        |    id BIGINT,
        |    type VARCHAR,
        |    app_id VARCHAR,
        |    tenant_id BIGINT,
        |    tenant_line_id BIGINT,
        |    call_id VARCHAR,
        |    remote_queue_id VARCHAR,
        |    queue_id BIGINT,
        |    user_id BIGINT,
        |    consultant_id BIGINT,
        |    work_no VARCHAR,
        |    ext_num VARCHAR,
        |    `called` VARCHAR,
        |    caller VARCHAR,
        |    display_number VARCHAR,
        |    call_direct INT,
        |    ring_time TIMESTAMP,
        |    answer_time TIMESTAMP,
        |    end_time TIMESTAMP,
        |    ring_time_len INT,
        |    talk_time_len INT,
        |    total_time_len INT,
        |    drop_cause VARCHAR,
        |    hungup_flag INT,
        |    record_url VARCHAR,
        |    local_record_url VARCHAR,
        |    create_ts TIMESTAMP
        |) WITH (
        |    'connector' = 'mysql-cdc',
        |    'hostname' = 'hadoop101',
        |    'port' = '3306',
        |    'username' = 'root',
        |    'password' = 'yang156122',
        |    'database-name' = 'callcenter',
        |    'table-name' = 'call_record'
        |)
    """.stripMargin


    // sink数据至es
    //	    callcenter_called VARCHAR,
    val createSinkTabel =
      """
        |CREATE TABLE flinksql (
        |	    id BIGINT,
        |	    opportunity_table_id BIGINT,
        |	    opportunity_user_id BIGINT,
        |	    opportunity_user_account VARCHAR,
        |	    opportunity_student_id BIGINT,
        |	    opportunity_province_id BIGINT,
        |	    opportunity_city_id BIGINT,
        |	    opportunity_create_user_account VARCHAR,
        |	    opportunity_create_user_id BIGINT,
        |	    opportunity_create_time TIMESTAMP,
        |	    opportunity_allocate_time TIMESTAMP,
        |	    opportunity_apply_time TIMESTAMP,
        |	    opportunity_friend_time TIMESTAMP,
        |	    opportunity_site_id VARCHAR,
        |	    opportunity_site_source VARCHAR,
        |	    opportunity_intention VARCHAR,
        |	    opportunity_state VARCHAR,
        |	    opportunity_first_proj_id BIGINT,
        |	    opportunity_oppor_source VARCHAR,
        |	    opportunity_update_user_id BIGINT,
        |	    opportunity_update_time TIMESTAMP,
        |	    opportunity_first_valid_follow_time TIMESTAMP,
        |	    opportunity_valid_follow_time TIMESTAMP,
        |	    opportunity_valid_follow_counts INT,
        |	    opportunity_follow_time TIMESTAMP,
        |	    opportunity_abandon_status INT,
        |	    opportunity_abandon_time TIMESTAMP,
        |	    opportunity_silence_status INT,
        |	    opportunity_hidden_status INT,
        |	    opportunity_second_consult_status INT,
        |	    opportunity_advertiser_code VARCHAR,
        |	    opportunity_advertiser VARCHAR,
        |	    opportunity_get_status INT,
        |	    opportunity_get_time TIMESTAMP,
        |	    opportunity_delete_flag INT,
        |	    opportunity_org_id1 BIGINT,
        |	    opportunity_org_id2 BIGINT,
        |	    opportunity_org_id3 BIGINT,
        |	    opportunity_org_id4 BIGINT,
        |	    opportunity_org_id5 BIGINT,
        |	    opportunity_tenant_id BIGINT,
        |	    opportunity_code VARCHAR,
        |	    opportunity_last_follow_consultant_id BIGINT,
        |	    record_id INT,
        |	    record_opportunity_id BIGINT,
        |	    record_call_time TIMESTAMP,
        |	    record_type VARCHAR,
        |	    record_record_status INT,
        |	    record_create_time TIMESTAMP,
        |	    record_mobile VARCHAR,
        |	    record_call_id VARCHAR,
        |	    record_call_duration INT,
        |	    record_stage INT,
        |	    callcenter_id BIGINT,
        |	    callcenter_type VARCHAR,
        |	    callcenter_app_id VARCHAR,
        |	    callcenter_tenant_id BIGINT,
        |	    callcenter_tenant_line_id BIGINT,
        |	    callcenter_call_id VARCHAR,
        |	    callcenter_remote_queue_id VARCHAR,
        |	    callcenter_queue_id BIGINT,
        |	    callcenter_user_id BIGINT,
        |	    callcenter_consultant_id BIGINT,
        |	    callcenter_work_no VARCHAR,
        |	    callcenter_ext_num VARCHAR,
        |     callcenter_called VARCHAR,
        |	    callcenter_caller VARCHAR,
        |	    callcenter_display_number VARCHAR,
        |	    callcenter_call_direct INT,
        |	    callcenter_ring_time TIMESTAMP,
        |	    callcenter_answer_time TIMESTAMP,
        |	    callcenter_end_time TIMESTAMP,
        |	    callcenter_ring_time_len INT,
        |	    callcenter_talk_time_len INT,
        |	    callcenter_total_time_len INT,
        |	    callcenter_drop_cause VARCHAR,
        |	    callcenter_hungup_flag INT,
        |	    callcenter_record_url VARCHAR,
        |	    callcenter_local_record_url VARCHAR,
        |	    callcenter_create_ts TIMESTAMP,
        |     primary key(id) not ENFORCED
        |) WITH (
        |    'connector' = 'elasticsearch-6',
        |    'hosts' = 'http://123.207.27.238:9200',
        |    'index' = 'flinksql',
        |    'document-type' = '_doc',
        |    'format'='json',
        |    'sink.bulk-flush.max-actions'='5'
        |)
    """.stripMargin

    //连表sql
    //        cr.called as callcenter_called,
    val unoinSql =
      """
        |insert into flinksql
        |SELECT
        |        top.id as id,
        |        top.id as opportunity_table_id ,
        |        top.user_id as opportunity_user_id,
        |        top.user_account as opportunity_user_account,
        |        top.student_id as opportunity_student_id ,
        |        top.province_id as opportunity_province_id ,
        |        top.city_id as opportunity_city_id,
        |        top.create_user_account as opportunity_create_user_account,
        |        top.create_user_id as opportunity_create_user_id,
        |        top.create_time as opportunity_create_time,
        |        top.allocate_time as opportunity_allocate_time,
        |        top.apply_time as  opportunity_apply_time,
        |        top.friend_time as opportunity_friend_time,
        |        top.site_id as opportunity_site_id,
        |        top.site_source as opportunity_site_source,
        |        top.intention as opportunity_intention,
        |        top.state as opportunity_state,
        |        top.first_proj_id as opportunity_first_proj_id ,
        |        top.oppor_source as opportunity_oppor_source ,
        |        top.update_user_id as opportunity_update_user_id,
        |        top.update_time as opportunity_update_time,
        |        top.first_valid_follow_time as opportunity_first_valid_follow_time,
        |        top.valid_follow_time as opportunity_valid_follow_time ,
        |        top.valid_follow_counts as  opportunity_valid_follow_counts,
        |        top.follow_time as  opportunity_follow_time,
        |        top.abandon_status as opportunity_abandon_status ,
        |        top.abandon_time as  opportunity_abandon_time,
        |        top.silence_status as  opportunity_silence_status,
        |        top.hidden_status as opportunity_hidden_status ,
        |        top.second_consult_status as  opportunity_second_consult_status,
        |        top.advertiser_code as opportunity_advertiser_code,
        |        top.advertiser as opportunity_advertiser,
        |        top.get_status as opportunity_get_status ,
        |        top.get_time as  opportunity_get_time,
        |        top.delete_flag as opportunity_delete_flag ,
        |        top.org_id1 as opportunity_org_id1 ,
        |        top.org_id2 as opportunity_org_id2 ,
        |        top.org_id3 as opportunity_org_id3 ,
        |        top.org_id4 as opportunity_org_id4 ,
        |        top.org_id5 as opportunity_org_id5 ,
        |        top.tenant_id as opportunity_tenant_id ,
        |        top.opportunity_code as opportunity_code,
        |        top.last_follow_consultant_id as opportunity_last_follow_consultant_id,
        |        tcr.id as record_id,
        |        tcr.opportunity_id as  record_opportunity_id,
        |        tcr.call_time as  record_call_time,
        |        tcr.type as  record_type,
        |        tcr.record_status as record_record_status,
        |        tcr.create_time as  record_create_time,
        |        tcr.mobile as record_mobile,
        |        tcr.call_id as record_call_id,
        |        tcr.call_duration as record_call_duration,
        |        tcr.stage as record_stage,
        |        cr.id   as callcenter_id,
        |        cr.type as callcenter_type ,
        |        cr.app_id as callcenter_app_id,
        |        cr.tenant_id as callcenter_tenant_id,
        |        cr.tenant_line_id as callcenter_tenant_line_id,
        |        cr.call_id as callcenter_call_id,
        |        cr.remote_queue_id as callcenter_remote_queue_id ,
        |        cr.queue_id as callcenter_queue_id ,
        |        cr.user_id as callcenter_user_id,
        |        cr.consultant_id as callcenter_consultant_id,
        |        cr.work_no as callcenter_work_no,
        |        cr.ext_num as callcenter_ext_num,
        |        cr.`called` as callcenter_called,
        |        cr.caller as callcenter_caller,
        |        cr.display_number as callcenter_display_number,
        |        cr.call_direct as callcenter_call_direct,
        |        cr.ring_time as callcenter_ring_time,
        |        cr.answer_time as callcenter_answer_time,
        |        cr.end_time as callcenter_end_time,
        |        cr.ring_time_len as callcenter_ring_time_len,
        |        cr.talk_time_len as callcenter_talk_time_len,
        |        cr.total_time_len as callcenter_total_time_len,
        |        cr.drop_cause as  callcenter_drop_cause,
        |        cr.hungup_flag as callcenter_hungup_flag,
        |        cr.record_url as  callcenter_record_url,
        |        cr.local_record_url as  callcenter_local_record_url,
        |        cr.create_ts as  callcenter_create_ts
        |  FROM  t_opportunity  top
        |  LEFT JOIN t_call_record as tcr on top.id = tcr.opportunity_id
        |  LEFT JOIN call_record  cr ON tcr.call_id = cr.call_id
      """.stripMargin

    //执行sql
    tableEnv.executeSql(createTabelTOpportunity)
    tableEnv.executeSql(createTabelTCallRecord)
    tableEnv.executeSql(createTabelCallRecord)
    tableEnv.executeSql(createSinkTabel)
    val result: TableResult = tableEnv.executeSql(unoinSql)
    result.print()

  }
}
