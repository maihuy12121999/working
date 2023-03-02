package rever.etl.data_sync

import rever.etl.data_sync.jobs.rever_search
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.utils.Utils

import java.time.ZoneId
import java.util.TimeZone
import scala.collection.JavaConverters.mapAsJavaMapConverter

/** @author anhlt (andy)
  */
object PropertyHistoryESToClickhouseRunner {
  TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Asia/Saigon")))

  def main(args: Array[String]): Unit = {

    val config = Config
      .builder()
      .addArgument("RV_ES_SERVERS", true)
      .addArgument("RV_ES_CLUSTER_NAME", true)
      .addArgument("RV_ES_INDEX_NAME", true)
      .addArgument("RV_ES_TRAFFIC_SNIFF", false)
      .addArgument("RV_RAP_INGESTION_HOST", true)
      .addArgument("property_historical_topic", true)
      .addArgument("merge_after_write", false)
      .addArgument("is_sync_all", false)
      .build(Utils.parseArgToMap(args).asJava, true)
    rever_search.PropertyHistoryToCH(config).run()

  }
}
