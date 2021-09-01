package hive

import org.apache.flink.streaming.api.scala._
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.bridge.scala.StreamTableEnvironment
import org.apache.flink.table.catalog.hive.HiveCatalog
/**
 * @program: FlinkSql
 * @description: ${description}
 * @author: yang
 * @create: 2021-08-19 17:03
 */
object Demo1ComputeGenderIndex {
  def main(args: Array[String]): Unit = {
    val bsEnv: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val bsSettings: EnvironmentSettings = EnvironmentSettings
      .newInstance()
      .useBlinkPlanner() //使用blink计划器
      .inStreamingMode() //流处理模型
      .build()

    //创建table环境
    val bsTableEnv: StreamTableEnvironment = StreamTableEnvironment.create(bsEnv, bsSettings)

    val hiveCatalog: HiveCatalog = new HiveCatalog("myHive", "default", "/etc/hive/conf.cloudera.hive/")

    bsTableEnv.registerCatalog("myHive", hiveCatalog)

    bsTableEnv.useCatalog("myHive")

    bsTableEnv.executeSql(
      """
        |insert into test_1m_test2 values(4,'lhm',990)
      """.stripMargin)
  }


  }
