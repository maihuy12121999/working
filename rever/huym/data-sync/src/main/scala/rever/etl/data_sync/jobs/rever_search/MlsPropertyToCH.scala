package rever.etl.data_sync.jobs.rever_search

import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import rever.etl.data_sync.core.clickhouse.ClickhouseSink
import rever.etl.data_sync.core.es.EsSource
import rever.etl.data_sync.core.{EsToClickhouseFlow2, Runner}
import rever.etl.data_sync.domain.rever_search.MlsPropertyDM
import rever.etl.data_sync.normalizer.MlsPropertyNormalizer
import rever.etl.rsparkflow.api.configuration.Config

/** @author anhlt (andy)
  * @since 06/07/2022
  */
case class MlsPropertyToCH(config: Config) extends Runner {

  private val propertyTable = config.get("target_mls_property_table")
  private val isSyncAll = config.getBoolean("is_sync_all", false)

  override def run(): Long = {
    val source = EsSource(
      config,
      esType = "mls_listing",
      query = buildHourlyQuery(),
      timeField = "updated_time",
      200
    )

    val sink = ClickhouseSink(config, propertyTable, MlsPropertyDM.PRIMARY_IDS, MlsPropertyDM.FIELDS, 500)
    val normalizer = MlsPropertyNormalizer()

    val flow = EsToClickhouseFlow2(
      source,
      normalizer,
      sink
    )

    val totalSynced = flow.run()

    sink.mergeDuplicationHourly()

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
        .must(QueryBuilders.rangeQuery("updated_time").gte(fromTime).lt(toTime))
    }

  }

}
