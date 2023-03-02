package rever.etl.engagement.call

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import rever.etl.engagement.call.reader.{CallReader, UserHistoricalReader}
import rever.etl.engagement.call.writer.CallWriter
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.{Output, Table}
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.extensions.ByIntervalReportTemplate

class Call extends FlowMixin with ByIntervalReportTemplate {
  @Output(writer = classOf[CallWriter])
  @Table("engagement_call")
  def build(
      @Table(
        name = "engagement_call_historical",
        reader = classOf[CallReader]
      ) rawCallDf: DataFrame,
      @Table(
        name = "current_a0_users",
        reader = classOf[UserHistoricalReader]
      ) currentA0UserDf: DataFrame,
      config: Config
  ): DataFrame = {
    val reportTime = config.getDailyReportTime
    val dailyCallDf = normalizeColumns(rawCallDf, currentA0UserDf).persist(StorageLevel.MEMORY_AND_DISK_2)

    val previousA0Df = getPreviousA0DfFromS3(config.getJobId, reportTime, config)
    val a0Df = buildA0Df(previousA0Df, dailyCallDf, config)

    exportA1DfToS3(config.getJobId, reportTime, dailyCallDf, config)
    exportA0DfToS3(config.getJobId, reportTime, a0Df, config)

    val anDf = buildAnDf(previousA0Df, dailyCallDf)

    calcAnByInterval(
      config.getJobId,
      anDf,
      reportTime,
      Seq(
        CallHelper.BUSINESS_UNIT,
        CallHelper.MARKET_CENTER_ID,
        CallHelper.TEAM_ID,
        CallHelper.USER_TYPE,
        CallHelper.AGENT_ID,
        CallHelper.CREATOR_ID,
        CallHelper.SOURCE,
        CallHelper.CID,
        CallHelper.DIRECTION,
        CallHelper.CALL_STATUS
      ),
      Map(
        countDistinct(col(CallHelper.CALL_ID)) -> CallHelper.TOTAL_CALL,
        sum(col(CallHelper.DURATION)) -> CallHelper.TOTAL_DURATION,
        min(col(CallHelper.DURATION)) -> CallHelper.MIN_DURATION,
        max(col(CallHelper.DURATION)) -> CallHelper.MAX_DURATION
      ),
      CallHelper.generateRow,
      config,
      exportAnDataset = true
    )
  }

  private def normalizeColumns(rawCallDf: DataFrame, currentA0UserDf: DataFrame): DataFrame = {
    val callDf = rawCallDf
      .sort(col(CallHelper.TIMESTAMP).desc)
      .dropDuplicates(CallHelper.CALL_ID)

    val resultDf = explodeCids(callDf)
      .joinWith(currentA0UserDf, callDf(CallHelper.AGENT_ID) === currentA0UserDf("username"), "left")
      .select(
        expr("""(BIGINT(to_timestamp(to_date(timestamp_millis(_1.timestamp),"dd/MM/yyyy")))*1000)""").as(
          CallHelper.DATE
        ),
        col(s"_2.${CallHelper.BUSINESS_UNIT}"),
        col(s"_2.${CallHelper.MARKET_CENTER_ID}"),
        col(s"_2.${CallHelper.TEAM_ID}"),
        col(s"_2.${CallHelper.USER_TYPE}"),
        col(s"_1.${CallHelper.AGENT_ID}"),
        col(s"_1.${CallHelper.CREATOR_ID}"),
        col(s"_1.${CallHelper.SOURCE}"),
        col(s"_1.${CallHelper.CID}"),
        col(s"_1.${CallHelper.DIRECTION}"),
        col(s"_1.${CallHelper.CALL_STATUS}"),
        col(s"_1.${CallHelper.DURATION}"),
        col(s"_1.${CallHelper.CALL_ID}")
      )

    resultDf.na
      .fill("unknown")
      .na
      .replace(
        Seq(
          CallHelper.BUSINESS_UNIT,
          CallHelper.MARKET_CENTER_ID,
          CallHelper.TEAM_ID,
          CallHelper.USER_TYPE,
          CallHelper.AGENT_ID,
          CallHelper.CREATOR_ID,
          CallHelper.SOURCE,
          CallHelper.CID,
          CallHelper.DIRECTION,
          CallHelper.CALL_STATUS,
          CallHelper.DURATION,
          CallHelper.CALL_ID
        ),
        Map("" -> "unknown")
      )
  }

  private def explodeCids(rawCallDf: DataFrame): DataFrame = {
    rawCallDf
      .withColumn(
        CallHelper.CIDS,
        regexp_replace(col(CallHelper.CIDS), "[\\[\\'\\'\\]]", "")
      )
      .withColumn(CallHelper.CIDS, split(col(CallHelper.CIDS), ","))
      .withColumn(CallHelper.CID, explode(col(CallHelper.CIDS)))
      .drop(CallHelper.CIDS)
  }

  private def buildA0Df(previousA0Df: Option[DataFrame], dailyCallDf: DataFrame, config: Config): DataFrame = {
    previousA0Df match {
      case Some(previousA0Df) =>
        previousA0Df
          .unionByName(dailyCallDf, allowMissingColumns = true)
          .withColumn(CallHelper.DATE, lit(config.getDailyReportTime))
          .sort(col(CallHelper.CALL_ID).desc)
          .dropDuplicates(CallHelper.CALL_ID)
          .persist(StorageLevel.MEMORY_AND_DISK_2)
      case _ => dailyCallDf
    }
  }

  private def buildAnDf(previousA0Df: Option[DataFrame], dailyCallDf: DataFrame): DataFrame = {
    previousA0Df match {
      case Some(previousA0Df) =>
        dailyCallDf
          .join(
            previousA0Df,
            dailyCallDf(CallHelper.CALL_ID) === previousA0Df(CallHelper.CALL_ID),
            "leftanti"
          )
      case _ => dailyCallDf
    }
  }
}
