package rever.etl.agentcrm

import org.apache.spark.sql.SparkSession
import rever.etl.rsparkflow.RSparkFlow
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.api.udf.RUdfUtils
import rever.etl.rsparkflow.domain.SparkSessionImplicits.SparkSessionImplicits
import rever.etl.rsparkflow.utils.Utils

import java.time.ZoneId
import java.util.TimeZone
import scala.collection.JavaConverters.mapAsJavaMapConverter

object ActiveUserRunner {

  TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Asia/Saigon")))
  def main(args: Array[String]): Unit = {

    val config = Config
      .builder()
      .addArgument("CH_DRIVER", true)
      .addArgument("CH_HOST", true)
      .addArgument("CH_PORT", true)
      .addArgument("CH_USER_NAME", true)
      .addArgument("CH_PASSWORD", true)
      .addArgument("RV_S3_ACCESS_KEY", true)
      .addArgument("RV_S3_SECRET_KEY", true)
      .addArgument("RV_S3_REGION", true)
      .addArgument("RV_S3_BUCKET", true)
      .addArgument("RV_S3_PARENT_PATH", true)
      .addArgument("RV_RAP_INGESTION_HOST", true)
      .addArgument("RV_DATA_MAPPING_HOST", false)
      .addArgument("RV_DATA_MAPPING_USER", false)
      .addArgument("RV_DATA_MAPPING_PASSWORD", false)
      .addArgument("active_user_topic", true)
      .addArgument("agent_crm_domains", true)
      .addArgument("tech_team_ids", true)
      .addArgument("merge_after_write", false)
      .build(Utils.parseArgToMap(args).asJava, true)

    SparkSession
      .builder()
      .master("local[2]")
      .config(config.getSparkConfig)
      .getOrCreate()

//    SparkSession.active.sparkContext.setLogLevel("ERROR")
    SparkSession.active.initS3(config)
    RUdfUtils.registerAll(SparkSession.active)
    new RSparkFlow().run("rever.etl.agentcrm.active_user", config)
  }
}
