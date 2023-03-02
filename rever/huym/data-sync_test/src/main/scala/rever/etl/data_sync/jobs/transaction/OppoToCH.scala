package rever.etl.data_sync.jobs.transaction

import rever.etl.data_sync.core.Runner
import rever.etl.data_sync.core.clickhouse.ClickhouseSink
import rever.etl.data_sync.domain.transaction.Opportunity
import rever.etl.data_sync.normalizer.OppoNormalizer
import rever.etl.rsparkflow.api.configuration.Config

import java.util.concurrent.atomic.LongAdder

/** @author anhlt (andy)
  * @since 25/10/2022
  */
case class OppoToCH(config: Config) extends Runner {

  private val sourceOppoTable = config.get("source_oppo_table")
  private val targetOppoTable = config.get("target_oppo_table")
  private val isSyncAll = config.getBoolean("is_sync_all", false)

  override def run(): Long = {

    val source = OppoSource(
      config,
      sourceOppoTable,
      isSyncAll,
      200
    )

    val sink = ClickhouseSink(config, targetOppoTable, Opportunity.PRIMARY_IDS, Opportunity.FIELDS, 500)
    val normalizer = OppoNormalizer()

    syncOppoToClickHouse(source, normalizer, sink)
  }

  private def syncOppoToClickHouse(source: OppoSource, normalizer: OppoNormalizer, sink: ClickhouseSink): Long = {
    val counter = new LongAdder()

    source.pull((opportunity, _, totalRecords) => {
      normalizer
        .toRecord(opportunity, totalRecords)
        .foreach(record => {
          sink.write(record)
          counter.increment()
        })

    })

    sink.end()

    println(s"${getClass.getSimpleName}: Synced ${counter.longValue()} records")

    counter.longValue()
  }

}
