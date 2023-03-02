package rever.etl.data_sync.jobs.transaction

import rever.etl.data_sync.core.clickhouse.ClickhouseSink
import rever.etl.data_sync.core.mysql.MySqlSource
import rever.etl.data_sync.core.{MySQLToClickhouseFlow, Normalizer, Runner}
import rever.etl.data_sync.domain.{GenericRecord, Opportunity, OpportunityDM}
import rever.etl.data_sync.normalizer.GenericOppoNormalizer
import rever.etl.rsparkflow.api.configuration.Config

/** @author anhlt (andy)
  * @since 25/10/2022
  */
case class OppoToCH(config: Config) extends Runner {

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
      Opportunity.PRIMARY_IDS,
      Opportunity.FIELDS,
      Opportunity.UPDATED_TIME,
      fromTime,
      toTime,
      sourceBatchSize
    )
    val normalizer: Normalizer[GenericRecord, Map[String, Any]] = GenericOppoNormalizer()

    val sink = ClickhouseSink(config, targetTable, OpportunityDM.PRIMARY_IDS, OpportunityDM.FIELDS, targetBatchSize)

    val flow = MySQLToClickhouseFlow(source, normalizer, sink)
    val result = flow.run()

    sink.mergeDuplicationHourly()

    result
  }

}
