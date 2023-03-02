package rever.etl.emc

import org.apache.spark.sql.SparkSession
import rever.etl.rsparkflow.BaseRunner
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.api.udf.RUdfUtils
import rever.etl.rsparkflow.domain.SparkSessionImplicits.SparkSessionImplicits
import rever.etl.rsparkflow.utils.Utils

import java.time.ZoneId
import java.util.TimeZone
import scala.collection.JavaConverters.mapAsJavaMapConverter

object PersonalContactRunner extends BaseRunner {

  TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Asia/Saigon")))

  def main(args: Array[String]): Unit = {

    val config = Config
      .builder()
      .addClickHouseArguments()
      .addS3Arguments()
      .addDataMappingClientArguments()
      .addArgument("RV_RAP_INGESTION_HOST", true)
      .addArgument("user_historical_job_id", true)
      .addArgument("new_personal_contact_topic", true)
      .addArgument("total_personal_contact_topic", true)
      .addArgument("merge_after_write", false)
      .build(Utils.parseArgToMap(args).asJava, true)

    SparkSession
      .builder()
      .master("local[2]")
      .config(config.getSparkConfig)
      .getOrCreate()
    SparkSession.active.initS3(config)
    RUdfUtils.registerAll(SparkSession.active)

    val packageName = "rever.etl.emc.jobs.personal_contact"

    runWithDailyForceRun(packageName, config)
  }

}
