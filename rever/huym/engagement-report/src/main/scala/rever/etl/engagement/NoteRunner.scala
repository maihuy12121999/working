package rever.etl.engagement

import org.apache.spark.sql.SparkSession
import rever.etl.engagement.CallRunner.runWithDailyForceRun
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.api.udf.RUdfUtils
import rever.etl.rsparkflow.domain.SparkSessionImplicits.SparkSessionImplicits
import rever.etl.rsparkflow.utils.Utils
import rever.etl.rsparkflow.{BaseRunner, RSparkFlow}

import scala.collection.JavaConverters.mapAsJavaMapConverter

object NoteRunner extends BaseRunner {
  def main(args: Array[String]): Unit = {
    val config = Config
      .builder()
      .addClickHouseArguments()
      .addS3Arguments()
      .addDataMappingClientArguments()
      .addRapIngestionClientArguments()
      .addForceRunArguments()
      .addArgument("new_note_engagement_topic", true)
      .addArgument("rva_job_titles", true)
      .addArgument("sm_job_titles", true)
      .addArgument("sd_job_titles", true)
      .addArgument("lc_job_titles", true)
      .addArgument("merge_after_write", false)
      .build(Utils.parseArgToMap(args).asJava, true)

    SparkSession
      .builder()
      .master("local[2]")
      .getOrCreate()
    SparkSession.active.sparkContext.setLogLevel("ERROR")

    SparkSession.active.initS3(config)
    RUdfUtils.registerAll(SparkSession.active)

    runWithDailyForceRun("rever.etl.engagement.note", config)
  }
}
