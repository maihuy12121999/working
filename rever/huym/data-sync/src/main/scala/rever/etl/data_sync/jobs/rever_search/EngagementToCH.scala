package rever.etl.data_sync.jobs.rever_search

import org.elasticsearch.index.query.{QueryBuilder, QueryBuilders}
import rever.etl.data_sync.core.clickhouse.ClickhouseSink
import rever.etl.data_sync.core.es.EsSource
import rever.etl.data_sync.core.{EsToClickhouseFlow2, Runner}
import rever.etl.data_sync.domain.engagement._
import rever.etl.data_sync.normalizer.EngagementNormalizer
import rever.etl.rsparkflow.api.configuration.Config

/** @author anhlt (andy)
  * @since 06/07/2022
  */
case class EngagementToCH(config: Config) extends Runner {

  private val engagementType = config.get("engagement_type")
  private val targetEngagementTable = config.get("target_engagement_table")
  private val sourceBatchSize = config.getInt("source_batch_size", 200)
  private val targetBatchSize = config.getInt("target_batch_size", 500)
  private val isSyncAll = config.getBoolean("is_sync_all", false)

  override def run(): Long = {
    val source = EsSource(
      config,
      esType = "engagement",
      query = buildHourlyESQuery("updated_time", isSyncAll),
      timeField = "updated_time",
      sourceBatchSize
    )

    val normalizer = EngagementNormalizer(engagementType)

    val sink = engagementType match {
      case EngagementTypes.CALL =>
        ClickhouseSink(
          config,
          targetEngagementTable,
          CallDM.PRIMARY_IDS,
          CallDM.FIELDS,
          targetBatchSize
        )
      case EngagementTypes.EMAIL =>
        ClickhouseSink(
          config,
          targetEngagementTable,
          EmailDM.PRIMARY_IDS,
          EmailDM.FIELDS,
          targetBatchSize
        )
      case EngagementTypes.MEETING =>
        ClickhouseSink(
          config,
          targetEngagementTable,
          MeetingDM.PRIMARY_IDS,
          MeetingDM.FIELDS,
          targetBatchSize
        )
      case EngagementTypes.NOTE =>
        ClickhouseSink(
          config,
          targetEngagementTable,
          NoteDM.PRIMARY_IDS,
          NoteDM.FIELDS,
          targetBatchSize
        )
      case EngagementTypes.SURVEY =>
        ClickhouseSink(
          config,
          targetEngagementTable,
          SurveyDM.PRIMARY_IDS,
          SurveyDM.FIELDS,
          targetBatchSize
        )
      case _ => throw new Exception(s"Unknown engagement type: ${engagementType}")
    }

    val flow = EsToClickhouseFlow2(
      source,
      normalizer,
      sink
    )

    val totalSynced = flow.run()

    sink.mergeDuplicationHourly()

    totalSynced
  }

  private def buildHourlyESQuery(timeField: String, isSyncAll: Boolean = false): QueryBuilder = {

    if (isSyncAll) {
      QueryBuilders
        .boolQuery()
        .must(QueryBuilders.termQuery("type", engagementType))
    } else {
      val (fromTime, toTime) = config.getExecutionDateInfo.getSyncDataTimeRange
      QueryBuilders
        .boolQuery()
        .must(QueryBuilders.termQuery("type", engagementType))
        .must(QueryBuilders.rangeQuery(timeField).gte(fromTime).lt(toTime))
    }
  }

}
