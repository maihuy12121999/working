package rever.etl.support

import org.apache.spark.sql.SparkSession
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.api.udf.RUdfUtils
import rever.rsparkflow.spark.utils.Utils
import rever.rsparkflow.spark.{BaseRunner, RSparkFlow}

import java.time.ZoneId
import java.util.TimeZone
import scala.collection.JavaConverters.mapAsJavaMapConverter

object ListingMatchingDataFeatureStatsRunner extends BaseRunner {
  TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Asia/Saigon")))
  def main(args: Array[String]): Unit = {
    val config = Config
      .builder()
      .addClickHouseArguments()
      .addArgument("CH_DB", true)
      .addArgument("CH_TABLE", true)
      .addArgument("listing_matching_stats_config", true)
      .addArgument("numerical_features", true)
      .addArgument("categorical_features", true)
      .addArgument("saved_feature_type", true)
      .addArgument("model_version", true)
      .addArgument("merge_after_write", false)
      .build(Utils.parseArgToMap(args).asJava, true)

    val sparkSession = SparkSession
      .builder()
      .master("local[2]")
      .config(config.getSparkConfig)
      .getOrCreate()
    SparkSession.active.sparkContext.setLogLevel("ERROR")
    RUdfUtils.registerAll(sparkSession)

    new RSparkFlow().run("rever.etl.support.listing_matching.property_feature_stats", config)
  }
}
