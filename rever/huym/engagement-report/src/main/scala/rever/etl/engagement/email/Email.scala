package rever.etl.engagement.email

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import rever.etl.engagement.email.reader.{EmailReader, UserHistoricalReader}
import rever.etl.engagement.email.writer.EmailWriter
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.{Output, Table}
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.extensions.ByIntervalReportTemplate

class Email extends FlowMixin with ByIntervalReportTemplate {
  @Output(writer = classOf[EmailWriter])
  @Table("engagement_email")
  def build(
      @Table(
        name = "engagement_email_historical",
        reader = classOf[EmailReader]
      ) rawEmailDf: DataFrame,
      @Table(
        name = "current_a0_users",
        reader = classOf[UserHistoricalReader]
      ) currentA0UserDf: DataFrame,
      config: Config
  ): DataFrame = {

    val reportTime = config.getDailyReportTime
    val dailyEmailDf = normalizeColumns(rawEmailDf, currentA0UserDf)
      .persist(StorageLevel.MEMORY_AND_DISK_2)

    val previousA0Df = getPreviousA0DfFromS3(config.getJobId, reportTime, config)
      .map(_.persist(StorageLevel.MEMORY_AND_DISK_2))

    val a0Df = buildA0Df(previousA0Df, dailyEmailDf, config)

    exportA1DfToS3(config.getJobId, reportTime, dailyEmailDf, config)
    exportA0DfToS3(config.getJobId, reportTime, a0Df, config)

    val anDf = buildAnDf(previousA0Df, dailyEmailDf)

    calcAnByInterval(
      config.getJobId,
      anDf,
      reportTime,
      Seq(
        EmailHelper.BUSINESS_UNIT,
        EmailHelper.MARKET_CENTER_ID,
        EmailHelper.TEAM_ID,
        EmailHelper.USER_TYPE,
        EmailHelper.AGENT_ID,
        EmailHelper.CREATOR_ID,
        EmailHelper.CID,
        EmailHelper.DIRECTION,
        EmailHelper.EMAIL_STATUS
      ),
      Map(
        countDistinct(col(EmailHelper.EMAIL_ID)) -> EmailHelper.TOTAL_EMAIL
      ),
      EmailHelper.generateRow,
      config,
      exportAnDataset = true
    )
  }

  private def normalizeColumns(rawEmailDf: DataFrame, currentA0UserDf: DataFrame): DataFrame = {
    val emailDf = rawEmailDf
      .sort(col(EmailHelper.TIMESTAMP).desc)
      .dropDuplicates(EmailHelper.EMAIL_ID)

    val resultDf = explodeCids(emailDf)
      .joinWith(currentA0UserDf, emailDf(EmailHelper.AGENT_ID) === currentA0UserDf("username"), "left")
      .select(
        expr("""(BIGINT(to_timestamp(to_date(timestamp_millis(_1.timestamp),"dd/MM/yyyy")))*1000)""").as(
          EmailHelper.DATE
        ),
        col(s"_2.${EmailHelper.BUSINESS_UNIT}"),
        col(s"_2.${EmailHelper.MARKET_CENTER_ID}"),
        when(
          col(s"_1.${EmailHelper.TEAM_ID}").isNull
            .or(col(s"_1.${EmailHelper.TEAM_ID}").equalTo("")),
          col(s"_2.${EmailHelper.TEAM_ID}")
        )
          .otherwise(col(s"_1.${EmailHelper.TEAM_ID}"))
          .as(EmailHelper.TEAM_ID),
        col(s"_2.${EmailHelper.USER_TYPE}"),
        col(s"_1.${EmailHelper.AGENT_ID}"),
        col(s"_1.${EmailHelper.CREATOR_ID}"),
        col(s"_1.${EmailHelper.CID}"),
        col(s"_1.${EmailHelper.DIRECTION}"),
        col(s"_1.${EmailHelper.EMAIL_STATUS}"),
        col(s"_1.${EmailHelper.EMAIL_ID}")
      )
    resultDf.na
      .fill("unknown")
      .na
      .replace(
        Seq(
          EmailHelper.BUSINESS_UNIT,
          EmailHelper.MARKET_CENTER_ID,
          EmailHelper.TEAM_ID,
          EmailHelper.USER_TYPE,
          EmailHelper.AGENT_ID,
          EmailHelper.CREATOR_ID,
          EmailHelper.CID,
          EmailHelper.DIRECTION,
          EmailHelper.EMAIL_STATUS,
          EmailHelper.EMAIL_ID
        ),
        Map("" -> "unknown")
      )
  }

  private def explodeCids(rawEmailDf: DataFrame): DataFrame = {
    rawEmailDf
      .withColumn(
        EmailHelper.CIDS,
        regexp_replace(col(EmailHelper.CIDS), "[\\[\\'\\'\\]]", "")
      )
      .withColumn(EmailHelper.CIDS, split(col(EmailHelper.CIDS), ","))
      .withColumn(EmailHelper.CID, explode(col(EmailHelper.CIDS)))
      .drop(EmailHelper.CIDS)
  }

  private def buildA0Df(previousA0Df: Option[DataFrame], dailyEmailDf: DataFrame, config: Config): DataFrame = {
    previousA0Df match {
      case Some(previousA0Df) =>
        previousA0Df
          .unionByName(dailyEmailDf, allowMissingColumns = true)
          .withColumn(EmailHelper.DATE, lit(config.getDailyReportTime))
          .sort(col(EmailHelper.EMAIL_ID))
          .dropDuplicates(EmailHelper.EMAIL_ID)
          .persist(StorageLevel.MEMORY_AND_DISK_2)
      case _ => dailyEmailDf
    }
  }

  private def buildAnDf(previousA0Df: Option[DataFrame], dailyEmailDf: DataFrame): DataFrame = {
    previousA0Df match {
      case Some(previousA0Df) =>
        dailyEmailDf
          .join(
            previousA0Df,
            dailyEmailDf(EmailHelper.EMAIL_ID) === previousA0Df(EmailHelper.EMAIL_ID),
            "leftanti"
          )
      case _ => dailyEmailDf
    }
  }

}
