package rever.etl.data_sync

import rever.etl.data_sync.jobs.rever_search.EngagementToCH
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.utils.Utils

import java.time.ZoneId
import java.util.TimeZone
import scala.collection.JavaConverters.mapAsJavaMapConverter

/** @author anhlt (andy)
  */
object EngagementToCHRunner {
  TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Asia/Saigon")))

  def main(args: Array[String]): Unit = {

    val config = Config
      .builder()
      .addArgument("RV_ES_SERVERS", true)
      .addArgument("RV_ES_CLUSTER_NAME", true)
      .addArgument("RV_ES_INDEX_NAME", true)
      .addArgument("RV_ES_TRAFFIC_SNIFF", false)
      .addArgument("CH_DRIVER", true)
      .addArgument("CH_HOST", true)
      .addArgument("CH_PORT", true)
      .addArgument("CH_USER_NAME", true)
      .addArgument("CH_PASSWORD", true)
      .addArgument("CH_DB", true)
      .addArgument("engagement_type", true)
      .addArgument("target_engagement_table", true)
      .addArgument("source_batch_size", false)
      .addArgument("target_batch_size", false)
      .addArgument("is_sync_all", false)
      .addArgument("merge_after_write", false)
      .build(Utils.parseArgToMap(args).asJava, true)

    EngagementToCH(config).run()

  }
}
