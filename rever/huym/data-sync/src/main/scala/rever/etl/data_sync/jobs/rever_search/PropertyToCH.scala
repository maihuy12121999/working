package rever.etl.data_sync.jobs.rever_search

import rever.data_health.domain.listing.ListingScoreConfig
import rever.data_health.evaluators.EvaluatorBuilders
import rever.etl.data_sync.core.clickhouse.ClickhouseSink
import rever.etl.data_sync.core.es.EsSource
import rever.etl.data_sync.core.{EsToClickhouseFlow2, Runner}
import rever.etl.data_sync.domain.rever_search.PropertyDM
import rever.etl.data_sync.normalizer.PropertyNormalizer
import rever.etl.data_sync.util.Utils
import rever.etl.rsparkflow.api.configuration.Config
import vn.rever.common.util.JsonHelper

/** @author anhlt (andy)
  * @since 06/07/2022
  */
case class PropertyToCH(config: Config) extends Runner {

  private val propertyTable = config.get("target_property_table")
  private val sourceBatchSize = config.getInt("source_batch_size", 200)
  private val targetBatchSize = config.getInt("target_batch_size", 500)
  private val isSyncAll = config.getBoolean("is_sync_all", false)

  private val listingScoreEvaluator = Some(config.getAndDecodeBase64("DATA_HEALTH_LISTING_SCORE_CONFIG", ""))
    .filterNot(_.isEmpty)
    .map(JsonHelper.fromJson[ListingScoreConfig])
    .map(EvaluatorBuilders.rListingEvaluator)
    .getOrElse(EvaluatorBuilders.rListingEvaluator())

  override def run(): Long = {
    val source = EsSource(
      config,
      esType = "property",
      query = Utils.buildSyncDataESQueryByTimeRange(config.getExecutionDateInfo, "updated_time", isSyncAll),
      timeField = "updated_time",
      sourceBatchSize
    )

    val sink = ClickhouseSink(config, propertyTable, PropertyDM.PRIMARY_IDS, PropertyDM.FIELDS, targetBatchSize)
    val normalizer = PropertyNormalizer(listingScoreEvaluator)

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
