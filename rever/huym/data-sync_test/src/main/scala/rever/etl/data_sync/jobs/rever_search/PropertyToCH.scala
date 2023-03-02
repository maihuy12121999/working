package rever.etl.data_sync.jobs.rever_search

import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import rever.etl.data_sync.core.es.EsSource
import rever.etl.data_sync.core.rap.RapIngestionSink
import rever.etl.data_sync.core.{EsToClickhouseFlow, Runner}
import rever.etl.data_sync.normalizer.PropertyNormalizer
import rever.etl.rsparkflow.api.configuration.Config

import scala.concurrent.duration.DurationInt

/** @author anhlt (andy)
  * @since 06/07/2022
  */
case class PropertyToCH(config: Config) extends Runner {

  private val propertyTopic = config.get("property_topic")
  private val isSyncAll = config.getBoolean("is_sync_all", false)

  override def run(): Long = {
    val source = EsSource(
      config,
      esType = "property",
      query = buildHourlyQuery(),
      200
    )

    val sink = RapIngestionSink(propertyTopic, 50, config)
    val normalizer = PropertyNormalizer()

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
    val fromTime = config.getHourlyReportTime
    val toTime = fromTime + 1.hour.toMillis

    if (isSyncAll) {
      QueryBuilders
        .boolQuery()
        .must(QueryBuilders.matchAllQuery())
    } else {
      QueryBuilders
        .boolQuery()
        .must(QueryBuilders.rangeQuery("updated_time").gte(fromTime).lt(toTime))
    }

  }

}
