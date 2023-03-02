package rever.etl.agentcrm

import org.apache.spark.sql.SparkSession
import rever.etl.rsparkflow.RSparkFlow
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.api.udf.RUdfUtils
import rever.etl.rsparkflow.utils.Utils

import scala.collection.JavaConverters.mapAsJavaMapConverter

object PageviewAnalysisRunner {
  def main(args: Array[String]): Unit = {
    val config = Config
      .builder()
      .addArgument("CH_DRIVER", true)
      .addArgument("CH_HOST", true)
      .addArgument("CH_PORT", true)
      .addArgument("CH_USER_NAME", true)
      .addArgument("CH_PASSWORD", true)
      .addArgument("RV_JOB_ID", true)
      .addArgument("RV_EXECUTION_DATE", true)
      .addArgument("RV_DATA_MAPPING_HOST", true)
      .addArgument("RV_DATA_MAPPING_USER", true)
      .addArgument("RV_DATA_MAPPING_PASSWORD", true)
      .addArgument("RV_RAP_INGESTION_HOST", true)
      .addArgument("page_view_topic", true)
      .addArgument("tech_team_ids",true)
      .build(Utils.parseArgToMap(args).asJava, true)

     SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()
    SparkSession.active.sparkContext.setLogLevel("ERROR")
    RUdfUtils.registerAll(SparkSession.active)
    new RSparkFlow().run("rever.etl.agentcrm.page_view", config)
  }
}
