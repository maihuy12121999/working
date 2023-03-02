package rever.etl.emc.jobs.sys_contact

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, countDistinct, expr, when}
import org.apache.spark.storage.StorageLevel
import rever.etl.emc.domain.SysContactFields
import rever.etl.emc.jobs.sys_contact.reader.{SysContactReader, UserHistoricalReader}
import rever.etl.emc.jobs.sys_contact.writer.SysContactWriter
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.{Output, Table}
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.extensions.{ByIntervalReportTemplate, RapIngestWriterMixin}

class SysContactReport extends FlowMixin with ByIntervalReportTemplate with RapIngestWriterMixin {

  @Output(writer = classOf[SysContactWriter])
  @Table(name = "sys_contact")
  def build(
      @Table(
        name = "raw_sys_contact",
        reader = classOf[SysContactReader]
      ) rawSysContactDf: DataFrame,
      @Table(
        name = "current_a0_users",
        reader = classOf[UserHistoricalReader]
      ) currentA0UserDf: DataFrame,
      config: Config
  ): DataFrame = {

    val reportTime = config.getDailyReportTime

    val dailyDf = normalizeColumns(rawSysContactDf, currentA0UserDf)
      .select(
        col(SysContactFields.DATE),
        col(SysContactFields.BUSINESS_UNIT),
        col(SysContactFields.MARKET_CENTER_ID),
        col(SysContactFields.TEAM_ID),
        col(SysContactFields.USER_TYPE),
        col(SysContactFields.AGENT_ID),
        col(SysContactFields.STATUS),
        col(SysContactFields.CID)
      )
      .persist(StorageLevel.MEMORY_AND_DISK_2)

    val previousA0Df = getPreviousA0DfFromS3(config.getJobId, reportTime, config)
      .map(_.persist(StorageLevel.MEMORY_AND_DISK_2))

    val anDf = previousA0Df match {
      case Some(previousA0Df) =>
        dailyDf
          .join(previousA0Df, dailyDf(SysContactFields.CID) === previousA0Df(SysContactFields.CID), "leftanti")
          .persist(StorageLevel.MEMORY_AND_DISK_2)
      case None => dailyDf
    }

    val a0Df = previousA0Df match {
      case Some(previousA0Df) =>
        previousA0Df
          .unionByName(dailyDf, allowMissingColumns = true)
          .orderBy(col(SysContactFields.DATE).desc)
          .dropDuplicates(SysContactFields.CID)
          .persist(StorageLevel.MEMORY_AND_DISK_2)
      case None => dailyDf
    }

    exportAnDfToS3(config.getJobId, reportTime, anDf, config)
    exportA0DfToS3(config.getJobId, reportTime, a0Df, config)

    saveNewSysContact(anDf, reportTime, config)

    val totalContactResultDf = calcA0ByInterval(
      a0Df,
      reportTime,
      Seq(
        SysContactFields.BUSINESS_UNIT,
        SysContactFields.MARKET_CENTER_ID,
        SysContactFields.TEAM_ID,
        SysContactFields.USER_TYPE,
        SysContactFields.AGENT_ID,
        SysContactFields.STATUS
      ),
      Map(
        countDistinct(col(SysContactFields.CID)) -> SysContactFields.NUM_CONTACT
      ),
      SysContactSchema.generateRow,
      config,
      persistDataset = true
    )

    totalContactResultDf
  }

  private def saveNewSysContact(anDf: DataFrame, reportTime: Long, config: Config) = {
    val newContactResultDf = calcAnByInterval(
      config.getJobId,
      anDf,
      reportTime,
      Seq(
        SysContactFields.BUSINESS_UNIT,
        SysContactFields.MARKET_CENTER_ID,
        SysContactFields.TEAM_ID,
        SysContactFields.USER_TYPE,
        SysContactFields.AGENT_ID,
        SysContactFields.STATUS
      ),
      Map(
        countDistinct(col(SysContactFields.CID)) -> SysContactFields.NUM_CONTACT
      ),
      SysContactSchema.generateRow,
      config,
      exportAnDataset = false,
      persistDataset = true
    )

    val topic = config.get("new_sys_contact_topic")
    ingestDataframe(config, newContactResultDf, topic)(SysContactSchema.toNewSysContactRecord, 800)

    mergeIfRequired(config, newContactResultDf, topic)

  }

  private def normalizeColumns(rawSysContactDf: DataFrame, currentA0UserDf: DataFrame): DataFrame = {
    rawSysContactDf
      .joinWith(currentA0UserDf, rawSysContactDf("agent_id") === currentA0UserDf("username"), "left")
      .select(
        expr("""(BIGINT(to_timestamp(to_date(timestamp_millis(_1.timestamp),"dd/MM/yyyy")))*1000)""").as(
          SysContactFields.DATE
        ),
        col("_2.business_unit"),
        col("_2.market_center_id"),
        col("_2.team_id"),
        col("_2.user_type"),
        col("_1.agent_id"),
        col("_1.status"),
        col("_1.cid")
      )
      .withColumn(
        SysContactFields.BUSINESS_UNIT,
        when(col(SysContactFields.BUSINESS_UNIT).isNull.or(col(SysContactFields.BUSINESS_UNIT).equalTo("")), "unknown")
          .otherwise(col(SysContactFields.BUSINESS_UNIT))
      )
      .withColumn(
        SysContactFields.MARKET_CENTER_ID,
        when(
          col(SysContactFields.MARKET_CENTER_ID).isNull.or(col(SysContactFields.MARKET_CENTER_ID).equalTo("")),
          "unknown"
        )
          .otherwise(col(SysContactFields.MARKET_CENTER_ID))
      )
      .withColumn(
        SysContactFields.TEAM_ID,
        when(col(SysContactFields.TEAM_ID).isNull.or(col(SysContactFields.TEAM_ID).equalTo("")), "unknown")
          .otherwise(col(SysContactFields.TEAM_ID))
      )
      .withColumn(
        SysContactFields.USER_TYPE,
        when(col(SysContactFields.USER_TYPE).isNull.or(col(SysContactFields.USER_TYPE).equalTo("")), "unknown")
          .otherwise(col(SysContactFields.USER_TYPE))
      )
      .withColumn(
        SysContactFields.AGENT_ID,
        when(col(SysContactFields.AGENT_ID).isNull.or(col(SysContactFields.AGENT_ID).equalTo("")), "unknown")
          .otherwise(col(SysContactFields.AGENT_ID))
      )
  }

}
