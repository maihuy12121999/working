import org.apache.spark.sql.SparkSession
import rever.etl.rsparkflow.BaseRunner
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.utils.Utils

import java.time.ZoneId
import java.util.TimeZone
import scala.jdk.CollectionConverters.mapAsJavaMapConverter

object RestoreMissingOppoHistoryToCHRunner extends BaseRunner {
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
      .addArgument("gsheet_missing_oppo_service_account_key", true)
      .addArgument("gsheet_missing_oppo_spreadsheet_id", true)
      .addArgument("gsheet_missing_oppo_sheet_name", true)
      .addArgument("gsheet_missing_oppo_data_range", true)
      .addArgument("merge_after_write", false)
      .build(Utils.parseArgToMap(args).asJava, true)

    SparkSession
      .builder()
      .master("local[2]")
      .config(config.getSparkConfig)
      .getOrCreate()

    SparkSession.active.sparkContext.setLogLevel("ERROR")

    runWithDailyForceRun("restore_oppo_historical", config)
  }
}
