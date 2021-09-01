//package hive
//
//import org.apache.flink.table.api.{EnvironmentSettings, TableEnvironment}
//import org.apache.flink.table.catalog.hive.HiveCatalog
//
///**
// * @program: FlinkSql
// * @description: ${description}
// * @author: yang
// * @create: 2021-06-21 17:08
// */
//object HiveDemo {
//  def main(args: Array[String]): Unit = {
//    val settings = EnvironmentSettings.newInstance().useBlinkPlanner().build()
//    val tenv = TableEnvironment.create(settings)
//
//    val catalogName = "hive"
//    val defaultDatabase = "default"
//    val hiveConfDir = "E:\\Flink\\FlinkSql\\FlinkCdcHive\\src\\main\\resources"
//
//    val hive = new HiveCatalog(catalogName, defaultDatabase, hiveConfDir)
//    tenv.registerCatalog(catalogName, hive)
//    tenv.useCatalog(catalogName)
//
////    val sql = "insert into test_1m_test2 values(4,'lhm',990)"
//    val sql = "select *  from user_test"
//
//    val result = tenv.executeSql(sql)
//    result.print()
//
//   // println(result.getJobClient.get().getJobStatus)
//    tenv.execute("sdf")
//  }
//}
