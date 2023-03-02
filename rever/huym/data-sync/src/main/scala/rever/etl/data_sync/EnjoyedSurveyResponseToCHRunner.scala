package rever.etl.data_sync

import org.apache.spark.sql.SparkSession
import rever.etl.rsparkflow.BaseRunner
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.utils.Utils

import java.time.ZoneId
import java.util.TimeZone
import scala.jdk.CollectionConverters.mapAsJavaMapConverter

object EnjoyedSurveyResponseToCHRunner extends BaseRunner {
  TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Asia/Saigon")))

  def main(args: Array[String]): Unit = {

    val config = Config
      .builder()
      .addForceRunArguments()
      .addArgument("CH_DRIVER", true)
      .addArgument("CH_HOST", true)
      .addArgument("CH_PORT", true)
      .addArgument("CH_USER_NAME", true)
      .addArgument("CH_PASSWORD", true)
      .addArgument("CH_DB", true)
      .addArgument("MYSQL_DRIVER", true)
      .addArgument("MYSQL_HOST", true)
      .addArgument("MYSQL_PORT", true)
      .addArgument("MYSQL_USER_NAME", true)
      .addArgument("MYSQL_PASSWORD", true)
      .addArgument("MYSQL_DB", true)
      .addArgument("source_table", true)
      .addArgument("primary_pipeline_ids", true)
      .addArgument("secondary_pipeline_ids", true)
      .addArgument("phase_standardized_mapping", true)
      .addArgument("phase_to_stage_mapping", true)
      .addArgument("oppo_stage_to_phase_id_mapping", true)
      .addArgument("business_mapping", true)
      .addArgument("target_table", true)
      .addArgument("target_batch_size", false)
      .addArgument("is_sync_all", false)
      .addArgument("merge_after_write", false)
      .build(Utils.parseArgToMap(args).asJava, true)

    SparkSession
      .builder()
      .master("local[2]")
      .config(config.getSparkConfig)
      .getOrCreate()

    SparkSession.active.sparkContext.setLogLevel("ERROR")

    runWithDailyForceRun("rever.etl.data_sync.jobs.survey_response", config)
  }
}
