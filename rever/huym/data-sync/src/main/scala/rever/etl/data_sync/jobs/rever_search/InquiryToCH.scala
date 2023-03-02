package rever.etl.data_sync.jobs.rever_search

import rever.etl.data_sync.core.clickhouse.ClickhouseSink
import rever.etl.data_sync.core.es.EsSource
import rever.etl.data_sync.core.{EsToClickhouseFlow2, Runner}
import rever.etl.data_sync.domain.rever_search.Inquiry
import rever.etl.data_sync.normalizer.InquiryNormalizer
import rever.etl.data_sync.util.Utils
import rever.etl.rsparkflow.api.configuration.Config

/** @author anhlt (andy)
  * @since 06/07/2022
  */
case class InquiryToCH(config: Config) extends Runner {

  private val targetInquiryTable = config.get("target_inquiry_table")
  private val isSyncAll = config.getBoolean("is_sync_all", false)

  override def run(): Long = {
    val source = EsSource(
      config,
      esType = "list_inquiry",
      query = Utils.buildSyncDataESQueryByTimeRange(
        config.getExecutionDateInfo,
        "updated_time",
        isSyncAll
      ),
      timeField = "updated_time",
      200
    )

    val sink = ClickhouseSink(config, targetInquiryTable, Inquiry.PRIMARY_IDS, Inquiry.FIELDS, 500)
    val normalizer = InquiryNormalizer()

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
