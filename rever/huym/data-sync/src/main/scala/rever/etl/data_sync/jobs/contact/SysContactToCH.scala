package rever.etl.data_sync.jobs.contact

import rever.etl.data_sync.core.clickhouse.ClickhouseSink
import rever.etl.data_sync.core.mysql.MySqlSource
import rever.etl.data_sync.core.{MySQLToClickhouseFlow, Runner}
import rever.etl.data_sync.domain.contact.{SysContact, SysContactDM}
import rever.etl.rsparkflow.api.configuration.Config

/** @author anhlt (andy)
  * @since 06/07/2022
  */
case class SysContactToCH(config: Config) extends Runner {

  private val sourceTable = config.get("source_table")
  private val targetTable = config.get("target_table")
  private val sourceBatchSize = config.getInt("source_batch_size", 200)
  private val targetBatchSize = config.getInt("target_batch_size", 1000)
  private val isSyncAll = config.getBoolean("is_sync_all", false)

  override def run(): Long = {
    val (fromTime, toTime) = if (isSyncAll) {
      (0L, System.currentTimeMillis())
    } else {
      config.getExecutionDateInfo.getSyncDataTimeRange
    }

    val source = MySqlSource(
      config,
      sourceTable,
      SysContact.PRIMARY_IDS,
      SysContact.FIELDS,
      SysContact.UPDATED_TIME,
      fromTime,
      toTime,
      sourceBatchSize
    )

    val normalizer = SysContactNormalizer()

    val sink = ClickhouseSink(config, targetTable, SysContactDM.PRIMARY_IDS, SysContactDM.FIELDS, targetBatchSize)

    val flow = MySQLToClickhouseFlow(source, normalizer, sink)

    val totalSynced = flow.run()

    sink.mergeDuplicationHourly()

    totalSynced
  }

}
