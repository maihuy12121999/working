package rever.etl.data_sync

import rever.etl.data_sync.jobs.admin.OptimizeTableToCH
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.utils.Utils

import java.time.ZoneId
import java.util.TimeZone
import scala.collection.JavaConverters.mapAsJavaMapConverter

/** @author anhlt (andy)
  */
object AdminCHOptimizeTableRunner {
  TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Asia/Saigon")))

  def main(args: Array[String]): Unit = {

    val config = Config
      .builder()
      .addArgument("CH_DRIVER", true)
      .addArgument("CH_HOST", true)
      .addArgument("CH_PORT", true)
      .addArgument("CH_USER_NAME", true)
      .addArgument("CH_PASSWORD", true)
      .addArgument("CH_DB", true)
      .addArgument("optimize_database_watchlist", true)
      .addArgument("max_size_in_bytes", true)
      .build(Utils.parseArgToMap(args).asJava, true)

    OptimizeTableToCH(config).run()

  }
}
