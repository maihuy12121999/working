package rever.etl.support

import org.apache.spark.sql.SparkSession
import rever.rsparkflow.spark.BaseRunner
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.implicits.SparkSessionImplicits.SparkSessionImplicits
import rever.rsparkflow.spark.utils.Utils

import java.time.ZoneId
import java.util.TimeZone
import scala.collection.JavaConverters.mapAsJavaMapConverter

object InquiryDescriptionRunner extends BaseRunner{
  TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Asia/Saigon")))

  def main(args: Array[String]): Unit = {

    val config = Config
      .builder()
      .addArgument("CH_DRIVER", true)
      .addArgument("CH_HOST", true)
      .addArgument("CH_PORT", true)
      .addArgument("CH_USER_NAME", true)
      .addArgument("CH_PASSWORD", true)
      .addArgument("CH_DB", true)
      .addArgument("RV_S3_ACCESS_KEY", true)
      .addArgument("RV_S3_SECRET_KEY", true)
      .addArgument("RV_S3_REGION", true)
      .addArgument("RV_S3_BUCKET", true)
      .addArgument("RV_S3_PARENT_PATH", true)
      .addArgument("source_table", true)
      .addArgument("forcerun.enabled", true)
      .addArgument("forcerun.from_date", true)
      .addArgument("forcerun.to_date", true)
      .addArgument("merge_after_write", false)
      .build(Utils.parseArgToMap(args).asJava, true)

    SparkSession
      .builder()
      .master("local[2]")
      .config(config.getSparkConfig)
      .getOrCreate()
    SparkSession.active.sparkContext.setLogLevel("ERROR")
    SparkSession.active.initS3(config)
    runWithDailyForceRun("rever.etl.support.export_inquiry", config)
  }
}
