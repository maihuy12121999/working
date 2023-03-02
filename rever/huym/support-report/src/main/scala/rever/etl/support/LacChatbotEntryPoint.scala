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
  *  Export LAC
  *  https://docs.google.com/spreadsheets/u/1/d/1Uetl62uXzFmoBQCAs8X4qz3UkJ0l0ewDjYWeO-PcM0g/edit
  */
object LacChatbotEntryPoint {
  TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Asia/Saigon")))

  def main(args: Array[String]): Unit = {

    val config = Config
      .builder()
      .addArgument("CH_DRIVER", true)
      .addArgument("CH_HOST", true)
      .addArgument("CH_PORT", true)
      .addArgument("CH_USER_NAME", true)
      .addArgument("CH_PASSWORD", true)
      .addArgument("export_spreadsheet_id", true)
      .addArgument("export_sheet_name", true)
      .addArgument("export_start_row_index", true)
      .addArgument("export_sheet_column_count", true)
      .addArgument("export_sheet_start_edit_column_index", true)
      .addArgument("export_sheet_end_edit_column_index", true)
      .addArgument("export_google_service_account_base64", true)
      .build(Utils.parseArgToMap(args).asJava, true)

    SparkSession
      .builder()
      .master("local[2]")
      .config(config.getSparkConfig)
      .getOrCreate()

    new RSparkFlow().run("rever.etl.support.chatbot_lac", config)
  }
}
