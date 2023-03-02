package rever.etl.data_sync

import rever.etl.data_sync.jobs.transaction.OppoToCH
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.utils.Utils

import java.time.ZoneId
import java.util.TimeZone
import scala.collection.JavaConverters.mapAsJavaMapConverter

/** @author anhlt (andy)
  */
object OppoToCHRunner {
  TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Asia/Saigon")))

  def main(args: Array[String]): Unit = {

    val config = Config
      .builder()
      .addArgument("MYSQL_DRIVER", true)
      .addArgument("MYSQL_HOST", true)
      .addArgument("MYSQL_PORT", true)
      .addArgument("MYSQL_USER_NAME", true)
      .addArgument("MYSQL_PASSWORD", true)
      .addArgument("MYSQL_DB", true)
      .addArgument("CH_DRIVER", true)
      .addArgument("CH_HOST", true)
      .addArgument("CH_PORT", true)
      .addArgument("CH_USER_NAME", true)
      .addArgument("CH_PASSWORD", true)
      .addArgument("CH_DB", true)
      .addArgument("source_oppo_table", true)
      .addArgument("target_oppo_table", true)
      .addArgument("is_sync_all", false)
      .build(Utils.parseArgToMap(args).asJava, true)

    OppoToCH(config).run()

  }
}
