package rever.etl.data_sync.jobs.user

import rever.etl.data_sync.core.clickhouse.ClickhouseSink
import rever.etl.data_sync.core.mysql.MySqlSource
import rever.etl.data_sync.core.{MySQLToClickhouseFlow, Runner}
import rever.etl.data_sync.domain.{ReverUser, ReverUserDM}
import rever.etl.data_sync.normalizer.ReverUserNormalizer
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.client.DataMappingClient

import java.util
import scala.jdk.CollectionConverters.asScalaBufferConverter

/** @author anhlt (andy)
  * @since 06/07/2022
  */
case class ReverUserToCH(config: Config) extends Runner {

  private val sourceTable = config.get("source_table")
  private val targetTable = config.get("target_table")
  private val sourceBatchSize = config.getInt("source_batch_size", 200)
  private val targetBatchSize = config.getInt("target_batch_size", 500)
  private val agentJobTitles = config.getList("rva_job_titles", ",").asScala.toSet
  private val smJobTitles = config.getList("sm_job_titles", ",").asScala.toSet
  private val sdJobTitles = config.getList("sd_job_titles", ",").asScala.toSet
  private val isSyncAll = config.getBoolean("is_sync_all", false)

  override def run(): Long = {
    val (fromTime, toTime) = if (isSyncAll) {
      (0L, System.currentTimeMillis())
    } else {
      config.getExecutionDateInfo.getSyncDataTimeRange
    }

    val systemUserIds = getSystemUserIdFromEmailConfig(config)

    val source = MySqlSource(
      config,
      sourceTable,
      ReverUser.PRIMARY_IDS,
      ReverUser.FIELDS,
      ReverUser.UPDATED_TIME,
      fromTime,
      toTime,
      sourceBatchSize
    )

    val normalizer = ReverUserNormalizer(agentJobTitles, smJobTitles, sdJobTitles, systemUserIds)

    val sink = ClickhouseSink(config, targetTable, ReverUserDM.PRIMARY_IDS, ReverUserDM.FIELDS, targetBatchSize)

    val flow = MySQLToClickhouseFlow(source, normalizer, sink)

    val totalSynced = flow.run()

    sink.mergeDuplicationHourly()

    totalSynced
  }

  private def getSystemUserIdFromEmailConfig(config: Config): Set[String] = {
    val emails = config.getList("SYSTEM_USER_EMAILS", ",", util.Arrays.asList()).asScala.toSeq
    getUserIds(emails, config).toSet
  }

  private def getUserIds(emails: Seq[String], config: Config): Seq[String] = {
    val client = DataMappingClient.client(config)

    emails.distinct
      .grouped(100)
      .flatMap(emails => {
        client.mGetUserByEmail(emails).values.map(_.username)
      })
      .toSeq

  }

}
