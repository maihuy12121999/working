package rever.etl.data_sync.jobs.rever_search

import rever.etl.data_sync.core.clickhouse.ClickhouseSink
import rever.etl.data_sync.core.es.EsSource
import rever.etl.data_sync.core.{EsToClickhouseFlow2, Runner}
import rever.etl.data_sync.domain.rever_search.Area
import rever.etl.data_sync.normalizer.AreaNormalizer
import rever.etl.data_sync.util.Utils
import rever.etl.rsparkflow.api.configuration.Config

/** @author anhlt (andy)
  * @since 06/07/2022
  */
case class AreaToCH(config: Config) extends Runner {

  private val areaTable = config.get("target_area_table")
  private val isSyncAll = config.getBoolean("is_sync_all", false)

  override def run(): Long = {

    val source = EsSource(
      config,
      esType = "area",
      query = Utils.buildSyncDataESQueryByTimeRange(config.getExecutionDateInfo, "updated_time", isSyncAll),
      timeField = "updated_time",
      200
    )

    val sink = ClickhouseSink(config, areaTable, Area.PRIMARY_IDS, Area.FIELDS, 800)
    val normalizer = AreaNormalizer()

    val flow = EsToClickhouseFlow2(
      source,
      normalizer,
      sink
    )

    val totalSynced = flow.run()

    sink.mergeDuplicationHourly()

    totalSynced
  }

}
