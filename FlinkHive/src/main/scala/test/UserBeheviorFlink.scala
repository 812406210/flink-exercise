package test

import java.text.SimpleDateFormat
import java.time.Duration
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.{CheckpointingMode, TimeCharacteristic}
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.flink.streaming.api.scala._
/**
 * @program: FlinkSql
 * @description: ${description}
 * @author: yang
 * @create: 2021-08-20 10:56
 * 参考博客:https://blog.csdn.net/m0_37592814/article/details/108044830
 */

object UserBeheviorFlink {

  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv: StreamTableEnvironment = StreamTableEnvironment.create(env)
    env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    env.enableCheckpointing(60000, CheckpointingMode.EXACTLY_ONCE)



    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "uat-datacenter1:9092,uat-datacenter2:9092,uat-datacenter3:9092")
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.setProperty("key.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
    props.setProperty("value.deserializer",
      "org.apache.kafka.common.serialization.StringDeserializer")
//    props.put("group.id", "user-group")


    val inputStream: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("user_behavior", new SimpleStringSchema(), props))

    inputStream.print("yuan-data=====>")
    val stream: DataStream[UserBehavior] = inputStream.map(new MapFunction[String,UserBehavior] {
      override def map(value: String): UserBehavior = {
        val jsonObject: JSONObject = JSON.parseObject(value)
        val sessionId = jsonObject.getString("sessionId")
        val userId = jsonObject.getString("userId")
        val mtWmPoiId = jsonObject.getString("mtWmPoiId")
        val shopName = jsonObject.getString("shopName")
        val platform = jsonObject.getString("platform")
        val source = jsonObject.getString("source")
        val createTime = jsonObject.getString("createTime")
        val dt = formatDateByFormat(createTime,"yyyyMMdd")
        val hr = formatDateByFormat(createTime,"HH")
        val mm = formatDateByFormat(createTime,"mm")
        UserBehavior(sessionId,userId,mtWmPoiId,shopName,platform,source,createTime,dt,hr,mm)
      }
    })
      .assignTimestampsAndWatermarks(WatermarkStrategy.forBoundedOutOfOrderness[UserBehavior](Duration.ofSeconds(20))
      .withTimestampAssigner(new SerializableTimestampAssigner[UserBehavior] {
        val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        override def extractTimestamp(element: UserBehavior, recordTimestamp: Long): Long = {
          val create_time = element.create_time
          val date = dateFormat.parse(create_time)
          date.getTime
        }
      }))

    stream.print("stream-data1=====>")
    val name            = "myHive1"
    val defaultDatabase = "default"
    val hiveConfDir = "/etc/hive/conf.cloudera.hive/"
    val hive = new HiveCatalog(name,defaultDatabase,hiveConfDir)
    tableEnv.registerCatalog("myHive1", hive)
    tableEnv.useCatalog("myHive1")
    //流转为表
    tableEnv.createTemporaryView("user_behavior",stream)
    stream.print("stream-data2=====>")
    /*tableEnv.executeSql("""
                          |INSERT INTO ods_user_behavior_new
                          |SELECT session_id, user_id,mt_wm_poi_id,shop_name,source,platform,create_time,dt,hr,mm
                          |FROM user_behavior
                        """.stripMargin)*/

    tableEnv.executeSql("INSERT INTO ods_user_behavior_new SELECT session_id, user_id,mt_wm_poi_id,shop_name,source,platform,create_time,dt,hr,mm FROM user_behavior")
    stream.print("stream-data3=====>")
    print("执行成功")
    env.execute()

  }
  def formatDateByFormat (dateString:String,format: String ): String = {

    val sourceDateFormat: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    val date = sourceDateFormat.parse(dateString)
    val targetDateFormat: SimpleDateFormat = new SimpleDateFormat(format)
    val time = targetDateFormat.format(date)
    time
  }
  case class UserBehavior(session_id: String,user_id: String,mt_wm_poi_id: String,shop_name: String,source: String,platform: String,create_time:String,dt:String,hr:String,mm:String)

}
