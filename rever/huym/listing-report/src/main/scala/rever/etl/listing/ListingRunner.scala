package rever.etl.listing

import org.apache.spark.sql.SparkSession
import rever.etl.rsparkflow.{BaseRunner, RSparkFlow}
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.api.udf.RUdfUtils
import rever.etl.rsparkflow.domain.SparkSessionImplicits.SparkSessionImplicits
import rever.etl.rsparkflow.utils.Utils

import java.time.ZoneId
import java.util.TimeZone
import scala.collection.JavaConverters.mapAsJavaMapConverter

object ListingRunner  extends BaseRunner {
  TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Asia/Saigon")))
  def main(args: Array[String]): Unit = {

    val config = Config
      .builder()
      .addClickHouseArguments()
      .addS3Arguments()
      .addRapIngestionClientArguments()
      .addForceRunArguments()
      .addArgument("user_historical_job_id", true)
      .addArgument("new_listing_topic", true)
      .addArgument("total_listing_topic", true)
      .addArgument("merge_after_write", false)
      .build(Utils.parseArgToMap(args).asJava, true)

    val sparkSession = SparkSession
      .builder()
      .master("local[2]")
      .config(config.getSparkConfig)
      .getOrCreate()
    SparkSession.active.sparkContext.setLogLevel("ERROR")
    RUdfUtils.registerAll(sparkSession)
    sparkSession.initS3(config)

    runWithDailyForceRun("rever.etl.listing.new_total_listing", config)
  }
}
