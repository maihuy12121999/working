package rever.etl.support

import org.apache.spark.sql.SparkSession
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.api.udf.RUdfUtils
import rever.rsparkflow.spark.utils.Utils
import rever.rsparkflow.spark.{BaseRunner, RSparkFlow}

import java.time.ZoneId
import java.util.TimeZone
import scala.collection.JavaConverters.mapAsJavaMapConverter

object ListingMatchingDataFeatureVectorRunner extends BaseRunner {
  TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Asia/Saigon")))

  def main(args: Array[String]): Unit = {
    val config = Config
      .builder()
      .addClickHouseArguments()
      .addArgument("listing_matching_vector_config")
      .build(Utils.parseArgToMap(args).asJava, true)

    val sparkSession = SparkSession
      .builder()
      .master("local[2]")
      .config(config.getSparkConfig)
      .getOrCreate()

    SparkSession.active.sparkContext.setLogLevel("ERROR")
    RUdfUtils.registerAll(sparkSession)

    new RSparkFlow().run("rever.etl.support.listing_matching.property_feature_vector", config)
  }

}
