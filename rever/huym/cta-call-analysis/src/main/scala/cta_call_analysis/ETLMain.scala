package cta_call_analysis
import rever.etl.rsparkflow.api.configuration.Config
import org.apache.spark.sql.SparkSession
import rever.etl.rsparkflow.RSparkFlow

import scala.collection.JavaConverters.mapAsJavaMapConverter

object ETLMain {
  def main(args: Array[String]): Unit = {
    val config = Config
      .builder()
      .addArgument("CH_DRIVER",true)
      .addArgument("CH_HOST",true)
      .addArgument("CH_PORT",true)
      .addArgument("CH_USER_NAME",true)
      .addArgument("CH_PASSWORD",true)
      .addArgument("RV_EXECUTION_DATE",true)
      .addArgument("RV_DATA_MAPPING_HOST",true)
      .addArgument("RV_DATA_MAPPING_USER",true)
      .addArgument("RV_DATA_MAPPING_PASSWORD",true)
      .addArgument("CTA_EVENTS",true)
      .addArgument("DURATION_LIMIT_BETWEEN_NON_LOGGED_USER_CLICK_AND_CALL",true)
      .addArgument("DURATION_LIMIT_BETWEEN_LOGGED_USER_CLICK_AND_CALL",true)
      .build(
        Map[String,AnyRef](
          "CH_DRIVER"->"ru.yandex.clickhouse.ClickHouseDriver",
          "CH_HOST"->"172.31.24.169",
          "CH_PORT"->"8123",
          "CH_USER_NAME"->"r3v3r_etl_devteam",
          "CH_PASSWORD"->"AEEFEW34r3v3r12ervrSWSQHHI=",
          "RV_JOB_ID"->"cta-call-analysis",
          "RV_EXECUTION_DATE"->"2022-04-28T00:00:00+00:00",
          "RV_DATA_MAPPING_HOST"->"http://eks-svc.reverland.com:31462",
          "RV_DATA_MAPPING_USER"->"rever",
          "RV_DATA_MAPPING_PASSWORD"->"rever@123!",
          "CTA_EVENTS"->"AgentDetailV2_ClickAgentPhone,ProjectDetailV3_ClickRVA3CX,PropertyDetail_ClickRVA3CX",
          "DURATION_LIMIT_BETWEEN_NON_LOGGED_USER_CLICK_AND_CALL"-> "10000",
          "DURATION_LIMIT_BETWEEN_LOGGED_USER_CLICK_AND_CALL"-> "900000"
        ).asJava, true
      )
    val sparkSession  = SparkSession
      .builder()
      .master("local[4]")
      .getOrCreate()
    sparkSession.sparkContext.setLogLevel("ERROR")
    import sparkSession.implicits._
    new RSparkFlow().run("cta_call_analysis",config)
  }
}
