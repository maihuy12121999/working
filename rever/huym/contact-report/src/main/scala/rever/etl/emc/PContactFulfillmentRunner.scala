package rever.etl.emc

import org.apache.spark.sql.SparkSession
import rever.etl.rsparkflow.BaseRunner
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.api.udf.RUdfUtils
import rever.etl.rsparkflow.domain.SparkSessionImplicits.SparkSessionImplicits
import rever.etl.rsparkflow.utils.Utils

import java.time.ZoneId
import java.util.TimeZone
import scala.jdk.CollectionConverters.mapAsJavaMapConverter

object PContactFulfillmentRunner extends BaseRunner {
  TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Asia/Saigon")))

  def main(args: Array[String]): Unit = {

    val config = Config
      .builder()
      .addClickHouseArguments()
      .addS3Arguments()
      .addPushKafkaClientArguments()
      .addForceRunArguments()
      .addArgument("data_delivery_personal_contact_fulfillment_topic", true)
      .addArgument("push_to_kafka", true)
      .build(Utils.parseArgToMap(args).asJava, true)

    SparkSession
      .builder()
      .master("local[2]")
      .config(config.getSparkConfig)
      .config("spark.sql.legacy.allowUntypedScalaUDF", value = true)
      .getOrCreate()
    SparkSession.active.sparkContext.setLogLevel("ERROR")

    SparkSession.active.initS3(config)
    RUdfUtils.registerAll(SparkSession.active)

    runWithDailyForceRun("rever.etl.emc.enhancement.personal_contact", config)
  }
}
