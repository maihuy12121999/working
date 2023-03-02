package rever.etl.data_sync

import rever.etl.data_sync.jobs.finance.FinanceDataToCH
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.utils.Utils

import java.time.ZoneId
import java.util.TimeZone
import scala.collection.JavaConverters.mapAsJavaMapConverter

/** @author anhlt (andy)
  */
object FinanceDataRunner {
  TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Asia/Saigon")))

  def main(args: Array[String]): Unit = {

    val config = Config
      .builder()
      .addArgument("CH_HOST", true)
      .addArgument("CH_PORT", true)
      .addArgument("CH_USER_NAME", true)
      .addArgument("CH_PASSWORD", true)
      .addArgument("CH_DB", true)
      .addArgument("google_service_account_encoded", true)
      .addArgument("finance_data_sheet_id", true)
      .build(Utils.parseArgToMap(args).asJava, true)

    FinanceDataToCH(config).run()

  }
}
