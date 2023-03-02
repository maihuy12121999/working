package rever.etl.call

import org.apache.spark.sql.SparkSession
import rever.etl.rsparkflow.BaseRunner
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.api.udf.RUdfUtils
import rever.etl.rsparkflow.domain.SparkSessionImplicits.SparkSessionImplicits
import rever.etl.rsparkflow.utils.Utils

import scala.collection.JavaConverters.mapAsJavaMapConverter

object NonCallAgentRunner extends BaseRunner {
  def main(args: Array[String]): Unit = {
    val config = Config
      .builder()
      .addClickHouseArguments()
      .addS3Arguments()
      .addForceRunArguments()
      .addRapIngestionClientArguments()
      .addArgument("user_historical_job_id", true)
      .addArgument("non_call_user_topic", true)
      .addArgument("rva_job_titles", true)
      .addArgument("sm_job_titles", true)
      .addArgument("sd_job_titles", true)
      .build(Utils.parseArgToMap(args).asJava, true)

    SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()

    SparkSession.active.sparkContext.setLogLevel("ERROR")
    SparkSession.active.initS3(config)
    RUdfUtils.registerAll(SparkSession.active)

    runWithDailyForceRun("rever.etl.call.jobs.non_call_users", config)

  }
}
