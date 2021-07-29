import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.api._
import org.apache.flink.connector.jdbc._
import java.sql.{Date, PreparedStatement, Timestamp}
import java.util.Properties

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.serialization.SimpleStringSchema
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer
import org.apache.kafka.clients.consumer.{ConsumerConfig, KafkaConsumer}
import org.apache.kafka.common.serialization.StringDeserializer
/**
 * @program: FlinkSql
 * @description: ${description}
 * @author: yang
 * @create: 2021-07-05 16:28
 *         建表语句：
 *          CREATE TABLE tutorial.user_all
 *          (
 *          `id` Int32,
 *          `name` String,
 *          `create_time` DateTime,
 *          `etl_time` DateTime,
 *          `bid` Int32,
 *          `username` String,
 *          `password` String
 *          )
 *          ENGINE = ReplacingMergeTree()
 *          PRIMARY KEY id
 *          ORDER BY (id,create_time)
 */
case class UserAll(id:Int,name:String, create_time:Timestamp,etl_time:Timestamp,bid:Int, username:String,password:String)
object StreamingJobJson {
  def main(args: Array[String]): Unit = {
    val env = StreamExecutionEnvironment.getExecutionEnvironment
    val tableEnv = StreamTableEnvironment.create(env)
    env.setParallelism(1)

    val ckJdbcUrl = "jdbc:clickhouse://hadoop101:8123/tutorial"
    val ckUserName = "default"
    val ckPassword = ""
    val batchSize = 5


    val props = new Properties()
    props.setProperty(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "hadoop101:9092")
    props.setProperty(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest")
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, classOf[StringDeserializer].getName)
    val input: DataStream[String] = env.addSource(new FlinkKafkaConsumer[String]("sensor", new SimpleStringSchema(), props))

    //过滤
    val jsonStream: DataStream[JSONObject] = input.map(data => {JSON.parseObject(data)})
    val inputStream: DataStream[JSONObject] = jsonStream.filter(data => "+I".equals(data.getString("op")))

    //转换
    val resultStream: DataStream[UserAll] = inputStream.map(data => {
      val resultDATA: JSONObject = data.getJSONObject("data")
      UserAll (
        resultDATA.getIntValue("id"),
        resultDATA.getString("name"),
        resultDATA.getTimestamp("create_time"),
        resultDATA.getTimestamp("etl_time"),
//        new Date(resultDATA.getDate("create_time").getTime),
//        new Date(resultDATA.getDate("etl_time").getTime),
        resultDATA.getIntValue("bid"),
        resultDATA.getString("username"),
        resultDATA.getString("password")
      )
    })

    resultStream.print("结果数据====>")
    val resultTable: Table = tableEnv.fromDataStream(resultStream, 'id, 'name, 'create_time, 'etl_time, 'bid, 'username, 'password)

    val resultDataStream =
      tableEnv.toAppendStream[(Int,String, Timestamp,Timestamp,Int,String,String)](resultTable)

    val insertIntoCkSql =
      """
        |  INSERT INTO user_all (
        |    id, name,create_time,etl_time,bid,username,password
        |  ) VALUES (
        |    ?,?,?,?,?,?,?
        |  )
      """.stripMargin

    resultDataStream.addSink(
      JdbcSink.sink[(Int,String, Timestamp,Timestamp,Int,String,String)](
        insertIntoCkSql,
        new CkSinkBuilderJson,
        new JdbcExecutionOptions
          .Builder()
          .withMaxRetries(5)
          .withBatchIntervalMs(50)
          .withBatchSize(batchSize)
          .build(),
        new JdbcConnectionOptions.JdbcConnectionOptionsBuilder()
          .withDriverName("ru.yandex.clickhouse.ClickHouseDriver")
          .withUrl(ckJdbcUrl)
          .withUsername(ckUserName)
          .withPassword(ckPassword)
          .build()
      )
    )
    env.execute("ck测试")
  }
}


//手动实现 interface 的方式来传入相关 JDBC Statement build 函数
class CkSinkBuilderJson extends JdbcStatementBuilder[(Int,String, Timestamp,Timestamp,Int,String,String)] {
  def accept(ps: PreparedStatement, v: (Int,String, Timestamp,Timestamp,Int,String,String)): Unit = {
    ps.setInt(1, v._1)
    ps.setString(2, v._2)
    ps.setTimestamp(3, v._3)
    ps.setTimestamp(4, v._4)
    ps.setInt(5, v._5)
    ps.setString(6, v._6)
    ps.setString(7, v._7)

  }
}
