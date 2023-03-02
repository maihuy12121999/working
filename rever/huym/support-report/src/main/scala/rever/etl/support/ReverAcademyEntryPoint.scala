package rever.etl.support

import org.apache.spark.sql.SparkSession
import rever.rsparkflow.spark.RSparkFlow
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.utils.Utils

import java.time.ZoneId
import java.util.TimeZone
import scala.collection.JavaConverters.mapAsJavaMapConverter

/** @author anhlt (andy)
  *
  *  Academy Management
  *   + Export REVER staff for: https://docs.google.com/spreadsheets/d/1kXrG79jaDmK7a3V91E-SPQgJ7G2v__rFjj3p0GFrftM/edit#gid=0
  *   + Sync list of students
  */
object ReverAcademyEntryPoint {
  TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Asia/Saigon")))

  def main(args: Array[String]): Unit = {

    val config = Config
      .builder()
      .addArgument("CH_DRIVER", true)
      .addArgument("CH_HOST", true)
      .addArgument("CH_PORT", true)
      .addArgument("CH_USER_NAME", true)
      .addArgument("CH_PASSWORD", true)
      .addArgument("RV_DATA_MAPPING_HOST", true)
      .addArgument("RV_DATA_MAPPING_USER", true)
      .addArgument("RV_DATA_MAPPING_PASSWORD", true)
      .addArgument("ignore_staff_quit_before_date", false)
      .addArgument("export_spreadsheet_id", true)
      .addArgument("export_sheet_name", true)
      .addArgument("export_start_row_index", true)
      .addArgument("export_sheet_column_count", true)
      .addArgument("export_sheet_start_edit_column_index", true)
      .addArgument("export_sheet_end_edit_column_index", true)
      .addArgument("export_google_service_account_base64", true)
      .addArgument("rever_academy_student_topic", true)
      .build(Utils.parseArgToMap(args).asJava, true)
    SparkSession
      .builder()
      .master("local[2]")
      .config(config.getSparkConfig)
      .getOrCreate()

    new RSparkFlow().run("rever.etl.support.rever_academy", config)
  }
}
