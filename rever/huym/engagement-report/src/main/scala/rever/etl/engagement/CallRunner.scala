package rever.etl.engagement

import org.apache.spark.sql.SparkSession
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.domain.SparkSessionImplicits.SparkSessionImplicits
import rever.etl.rsparkflow.utils.Utils
import rever.etl.rsparkflow.{BaseRunner, RSparkFlow}

import java.time.ZoneId
import java.util.TimeZone
import scala.collection.JavaConverters.mapAsJavaMapConverter

object CallRunner extends BaseRunner {
  TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Asia/Saigon")))

  def main(args: Array[String]): Unit = {

    val config = Config
      .builder()
      .addClickHouseArguments()
      .addS3Arguments()
      .addDataMappingClientArguments()
      .addRapIngestionClientArguments()
      .addForceRunArguments()
      .addArgument("new_call_engagement_topic", true)
      .addArgument("merge_after_write", false)
      .build(Utils.parseArgToMap(args).asJava, true)

    SparkSession
      .builder()
      .master("local[2]")
      .config(config.getSparkConfig)
      .getOrCreate()
    SparkSession.active.sparkContext.setLogLevel("ERROR")

    SparkSession.active.initS3(config)
    runWithDailyForceRun("rever.etl.engagement.call", config)
  }
}
