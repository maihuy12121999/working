package rever.etl.data_sync.jobs.daily_checkin

import rever.etl.data_sync.core.clickhouse.ClickhouseSink
import rever.etl.data_sync.core.mysql.MySqlSource
import rever.etl.data_sync.core.{MySQLToClickhouseFlow, Runner}
import rever.etl.data_sync.domain.user_activity.{DailyCheckin, DailyCheckinDM}
import rever.etl.rsparkflow.api.configuration.Config

/** @author anhlt (andy)
  * @since 06/07/2022
  */
case class DailyCheckinToCH(config: Config) extends Runner {

  private val sourceTable = config.get("source_table")
  private val targetTable = config.get("target_table")
  private val sourceBatchSize = config.getInt("source_batch_size", 200)
  private val targetBatchSize = config.getInt("target_batch_size", 500)
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
      DailyCheckin.PRIMARY_IDS,
      DailyCheckin.FIELDS,
      DailyCheckin.UPDATED_TIME,
      fromTime,
      toTime,
      sourceBatchSize
    )

    val normalizer = DailyCheckinNormalizer()

    val sink = ClickhouseSink(
      config,
      targetTable,
      DailyCheckinDM.PRIMARY_IDS,
      DailyCheckinDM.FIELDS,
      targetBatchSize
    )

    val flow = MySQLToClickhouseFlow(source, normalizer, sink)

    val totalSynced = flow.run()

    sink.mergeDuplicationHourly()

    totalSynced
  }

}
