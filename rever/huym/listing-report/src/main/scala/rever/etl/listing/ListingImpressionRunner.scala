package rever.etl.listing

import org.apache.spark.sql.SparkSession
import rever.etl.rsparkflow.BaseRunner
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.api.udf.RUdfUtils
import rever.etl.rsparkflow.domain.SparkSessionImplicits.SparkSessionImplicits
import rever.etl.rsparkflow.utils.Utils

import java.time.ZoneId
import java.util.TimeZone
import scala.collection.JavaConverters.mapAsJavaMapConverter

object ListingImpressionRunner extends BaseRunner {
  TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Asia/Saigon")))
  def main(args: Array[String]): Unit = {

    val config = Config
      .builder()
      .addClickHouseArguments()
      .addS3Arguments()
      .addRapIngestionClientArguments()
      .addForceRunArguments()
      .addArgument("listing_view_impressions_topic", true)
      .addArgument("merge_after_write", false)
      .build(Utils.parseArgToMap(args).asJava, true)

    SparkSession
      .builder()
      .master("local[2]")
      .config(config.getSparkConfig)
      .getOrCreate()
    SparkSession.active.sparkContext.setLogLevel("ERROR")
    RUdfUtils.registerAll(SparkSession.active)
    SparkSession.active.initS3(config)
    runWithDailyForceRun("rever.etl.listing.view_impressions", config)
  }
}
