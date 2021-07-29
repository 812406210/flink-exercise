package test

import java.sql.Timestamp
import java.text.SimpleDateFormat

import com.alibaba.fastjson.{JSON, JSONArray, JSONObject}

/**
 * @program: FlinkSql
 * @description: ${description}
 * @author: yang
 * @create: 2021-01-28 17:20
 */
object Test {
  def main(args: Array[String]): Unit = {
        println("hello world")

    var str = "{\"result\":[{\"trainId\":\"04001\",\"totalOffset\":\"384\",\"desc\":\"列车联挂状态\\n（列车联挂继电器激活）\",\"timestamp\":\"1622078271484\"}],\"componentType\":\"JDQ\"}"

    val nObject: JSONObject = JSON.parseObject(str)
    val str1: String = nObject.getString("componentType")
    val value: JSONArray = nObject.getJSONArray("result") //.getJSONObject(0)
    //value.put("componentType",str1)
    for(i <- 0 until value.size() ){
      val json: JSONObject = value.getJSONObject(i)
    }


    var newTime = new Timestamp(1567267200)
    val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
    val timestampFormat: Timestamp = Timestamp.valueOf(format.format(newTime))
    println("时间格式",timestampFormat)


  }
}
