package hive

import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
import org.apache.flink.table.catalog.hive.HiveCatalog

/**
 * @program: FlinkSql
 * @description: ${description}
 * @author: yang
 * @create: 2021-06-21 17:08
 */
object HiveDemo {
  def main(args: Array[String]): Unit = {
    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().build()
    val tenv = TableEnvironment.create(settings)

    val name = "myhive" //表
    val defaultDatabase = "default"  // 库
    val hiveConfDir = "E:\\Flink\\FlinkSql\\FlinkCdcHive\\src\\main\\resources"

    val hive = new HiveCatalog(name, defaultDatabase, hiveConfDir)
    tenv.registerCatalog("myhive", hive)
    tenv.useCatalog("myhive")

    val sql = "insert into myhive values(3,'lhm',990)"
    val result = tenv.executeSql(sql)

    println(result.getJobClient.get().getJobStatus)
  }
}
