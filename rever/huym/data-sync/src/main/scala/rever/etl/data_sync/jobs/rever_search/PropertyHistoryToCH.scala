package rever.etl.data_sync.jobs.rever_search

import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import rever.etl.data_sync.core.es.EsSource
import rever.etl.data_sync.core.rap.RapIngestionSink
import rever.etl.data_sync.core.{EsToClickhouseFlow, Runner}
import rever.etl.data_sync.normalizer.TrackingDataNormalizer
import rever.etl.rsparkflow.api.configuration.Config

/** @author anhlt (andy)
  * @since 06/07/2022
  */
case class PropertyHistoryToCH(config: Config) extends Runner {

  private val propertyHistoricalTopic = config.get("property_historical_topic")
  private val isSyncAll = config.getBoolean("is_sync_all", false)

  override def run(): Long = {

    val source = EsSource(
      config,
      esType = "property",
      query = buildHourlyQuery(),
      timeField = "timestamp",
      200
    )

    val sink = RapIngestionSink(propertyHistoricalTopic, 50, config)
    val normalizer = TrackingDataNormalizer()

    val flow = EsToClickhouseFlow(
      source,
      sink,
      normalizer
    )

    val totalSynced = flow.run()

    sink.forceMergeHourlySync()

    totalSynced
  }

  private def buildHourlyQuery(): QueryBuilder = {
    val (fromTime, toTime) = config.getExecutionDateInfo.getSyncDataTimeRange

    if (isSyncAll) {
      QueryBuilders
        .boolQuery()
        .must(QueryBuilders.matchAllQuery())
    } else {
      QueryBuilders
        .boolQuery()
        .must(QueryBuilders.rangeQuery("timestamp").gte(fromTime).lt(toTime))
    }

  }

}
