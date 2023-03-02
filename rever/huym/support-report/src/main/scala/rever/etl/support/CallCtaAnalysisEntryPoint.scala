package rever.etl.support

import org.apache.spark.sql.SparkSession
import rever.rsparkflow.spark.RSparkFlow
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.api.udf.RUdfUtils
import rever.rsparkflow.spark.utils.Utils

import java.time.ZoneId
import java.util.TimeZone
import scala.collection.JavaConverters.mapAsJavaMapConverter

object CallCtaAnalysisEntryPoint {
  TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Asia/Saigon")))
  def main(args: Array[String]): Unit = {

    val config = Config
      .builder()
      .addArgument("CH_DRIVER", true)
      .addArgument("CH_HOST", true)
      .addArgument("CH_PORT", true)
      .addArgument("CH_USER_NAME", true)
      .addArgument("CH_PASSWORD", true)
      .addArgument("RV_RAP_INGESTION_HOST", true)
      .addArgument("RV_DATA_MAPPING_HOST", true)
      .addArgument("RV_DATA_MAPPING_USER", true)
      .addArgument("RV_DATA_MAPPING_PASSWORD", true)
      .addArgument("call_cta_events", true)
      .addArgument("cs2_call_cta_event_topic", true)
      .build(Utils.parseArgToMap(args).asJava, true)

    SparkSession
      .builder()
      .master("local[2]")
      .config(config.getSparkConfig)
      .getOrCreate()

    RUdfUtils.registerAll(SparkSession.active)
    new RSparkFlow().run("rever.etl.support.cs2_call_cta_analysis", config)
  }
}
