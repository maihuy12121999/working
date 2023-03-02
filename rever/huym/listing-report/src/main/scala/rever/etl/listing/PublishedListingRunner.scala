package rever.etl.listing

import org.apache.spark.sql.SparkSession
import rever.etl.rsparkflow.BaseRunner
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.api.udf.RUdfUtils
import rever.etl.rsparkflow.domain.SparkSessionImplicits.SparkSessionImplicits
import rever.etl.rsparkflow.utils.Utils

import scala.collection.JavaConverters.mapAsJavaMapConverter

object PublishedListingRunner extends BaseRunner {
  def main(args: Array[String]): Unit = {

    val config = Config
      .builder()
      .addClickHouseArguments()
      .addS3Arguments()
      .addDataMappingClientArguments()
      .addRapIngestionClientArguments()
      .addForceRunArguments()
      .addArgument("user_historical_job_id", true)
      .addArgument("published_listing_topic", true)
      .addArgument("published_listing_dataset_topic", true)
      .addArgument("rva_job_titles", true)
      .addArgument("sm_job_titles", true)
      .addArgument("sd_job_titles", true)
      .addArgument("lc_job_titles", true)
      .addArgument("republish_gap_duration", false)
      .build(Utils.parseArgToMap(args).asJava, true)

    SparkSession
      .builder()
      .config("spark.sql.legacy.allowUntypedScalaUDF", value = true)
      .master("local[2]")
      .getOrCreate()
    SparkSession.active.sparkContext.setLogLevel("ERROR")
    RUdfUtils.registerAll(SparkSession.active)
    SparkSession.active.initS3(config)

    runWithDailyForceRun("rever.etl.listing.published_listings", config)
  }

}
