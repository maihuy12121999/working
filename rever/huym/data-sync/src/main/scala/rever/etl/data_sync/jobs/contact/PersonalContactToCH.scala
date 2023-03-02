package rever.etl.data_sync.jobs.contact

import rever.data_health.domain.p_contact.ContactScoreConfig
import rever.data_health.evaluators.EvaluatorBuilders
import rever.etl.data_sync.core.clickhouse.ClickhouseSink
import rever.etl.data_sync.core.mysql.MySqlSource
import rever.etl.data_sync.core.{MySQLToClickhouseFlow, Normalizer, Runner}
import rever.etl.data_sync.domain.GenericRecord
import rever.etl.data_sync.domain.contact.{PContact, PContactDM}
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.client.PushKafkaClient
import vn.rever.common.util.JsonHelper

/** @author anhlt (andy)
  * @since 06/07/2022
  */
case class PersonalContactToCH(config: Config) extends Runner {

  private val sourceTable = config.get("source_table")
  private val targetTable = config.get("target_table")
  private val sourceBatchSize = config.getInt("source_batch_size", 200)
  private val targetBatchSize = config.getInt("target_batch_size", 1000)
  private val isSyncAll = config.getBoolean("is_sync_all", false)
  private val isPutKafka = config.getBoolean("is_put_kafka", false)
  private val pContactEnrichmentTopic = config.get("data_delivery_enrichment_p_contact_topic", "")

  private val pContactScoreEvaluator = Some(config.getAndDecodeBase64("DATA_HEALTH_PERSONAL_CONTACT_SCORE_CONFIG", ""))
    .filterNot(_.isEmpty)
    .map(JsonHelper.fromJson[ContactScoreConfig])
    .map(EvaluatorBuilders.pContactEvaluator)
    .getOrElse(EvaluatorBuilders.pContactEvaluator)

  override def run(): Long = {
    val (fromTime, toTime) = if (isSyncAll) {
      (0L, System.currentTimeMillis())
    } else {
      config.getExecutionDateInfo.getSyncDataTimeRange
    }

    val putKafkaClient = PushKafkaClient.client(config)

    val source = MySqlSource(
      config,
      sourceTable,
      PContact.PRIMARY_IDS,
      PContact.FIELDS,
      PContact.UPDATED_TIME,
      fromTime,
      toTime,
      sourceBatchSize
    )

    val normalizer = PContactNormalizer(
      pContactScoreEvaluator,
      isPutKafka,
      putKafkaClient,
      pContactEnrichmentTopic
    )

    val sink = ClickhouseSink(config, targetTable, PContactDM.PRIMARY_IDS, PContactDM.FIELDS, targetBatchSize)

    val sinkMap = Map[Normalizer[GenericRecord, Map[String, Any]], ClickhouseSink](
      normalizer -> sink
    )

    val flow = MySQLToClickhouseFlow(source, sinkMap)

    val totalSynced = flow.run()

    sink.mergeDuplicationHourly()

    totalSynced
  }

}
