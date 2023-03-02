package rever.etl.data_sync.jobs.call

import rever.etl.data_sync.core.clickhouse.ClickhouseSink
import rever.etl.data_sync.core.es.EsSource
import rever.etl.data_sync.core.{EsToClickhouseFlow2, Runner}
import rever.etl.data_sync.domain.{CallHistory, CallHistoryDM}
import rever.etl.data_sync.util.Utils
import rever.etl.rsparkflow.api.configuration.Config

/** @author anhlt (andy)
  * @since 06/07/2022
  */
case class CallHistoryToCH(config: Config) extends Runner {

  private val callHistoryTable = config.get("target_call_history_table")
  private val sourceBatchSize = config.getInt("source_batch_size", 200)
  private val targetBatchSize = config.getInt("target_batch_size", 500)
  private val isSyncAll = config.getBoolean("is_sync_all", false)

  override def run(): Long = {
    val source = EsSource(
      config,
      esType = "call",
      query = Utils.buildSyncDataESQueryByTimeRange(config.getExecutionDateInfo, CallHistory.UPDATED_TIME, isSyncAll),
      timeField = CallHistory.UPDATED_TIME,
      batchSize = sourceBatchSize
    )

    val normalizer = CallHistoryNormalizer(config)
    val sink = ClickhouseSink(
      config,
      callHistoryTable,
      CallHistoryDM.PRIMARY_IDS,
      CallHistoryDM.FIELDS,
      targetBatchSize
    )

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
