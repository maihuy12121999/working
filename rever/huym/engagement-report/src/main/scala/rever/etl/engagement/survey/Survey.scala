package rever.etl.engagement.survey

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import rever.etl.engagement.domain.SurveyFields
import rever.etl.engagement.survey.reader.SurveyReader
import rever.etl.engagement.survey.writer.SurveyWriter
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.{Output, Table}
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.extensions.ByIntervalReportTemplate

class Survey extends FlowMixin with ByIntervalReportTemplate {
  @Output(writer = classOf[SurveyWriter])
  @Table("survey_report")
  def build(
      @Table(
        name = "engagement_survey_historical",
        reader = classOf[SurveyReader]
      ) rawSurveyDf: DataFrame,
      @Table(
        name = "current_a0_users",
        reader = classOf[reader.UserHistoricalReader]
      ) currentA0UserDf: DataFrame,
      config: Config
  ): DataFrame = {
    val reportTime = config.getDailyReportTime
    val dailySurveyDf = normalizeColumns(rawSurveyDf, currentA0UserDf).persist(StorageLevel.MEMORY_AND_DISK_2)

    val previousA0Df = getPreviousA0DfFromS3(config.getJobId, reportTime, config)
      .map(_.persist(StorageLevel.MEMORY_AND_DISK_2))
    val a0Df = buildA0Df(previousA0Df, dailySurveyDf, config)
    val anDf = buildAnDf(previousA0Df, dailySurveyDf)
      .persist(StorageLevel.MEMORY_AND_DISK_2)

    exportAnDfToS3(config.getJobId, reportTime, anDf, config)
    exportA0DfToS3(config.getJobId, reportTime, a0Df, config)

    calcAnByInterval(
      config.getJobId,
      anDf,
      reportTime,
      Seq(
        SurveyFields.BUSINESS_UNIT,
        SurveyFields.MARKET_CENTER_ID,
        SurveyFields.TEAM_ID,
        SurveyFields.USER_TYPE,
        SurveyFields.AGENT_ID,
        SurveyFields.SOURCE,
        SurveyFields.CID,
        SurveyFields.TEMPLATE_ID,
        SurveyFields.CHANNEL,
        SurveyFields.SURVEY_STATUS
      ),
      Map(
        countDistinct(col(SurveyFields.SURVEY_ID)) -> SurveyFields.TOTAL_SURVEY
      ),
      SurveyHelper.generateRow,
      config,
      exportAnDataset = false
    )
  }

  private def normalizeColumns(rawSurveyDf: DataFrame, currentA0UserDf: DataFrame): DataFrame = {
    val rawSurveyDfUpdated = rawSurveyDf
      .sort(col(SurveyFields.TIMESTAMP).desc)
      .dropDuplicates(SurveyFields.SURVEY_ID)

    val surveyDf = explodeCids(rawSurveyDfUpdated)

    val resultDf = surveyDf
      .join(currentA0UserDf, surveyDf(SurveyFields.AGENT_ID) === currentA0UserDf("username"), "left")
      .select(
        expr(s"""(BIGINT(to_timestamp(to_date(timestamp_millis(${SurveyFields.TIMESTAMP}),"dd/MM/yyyy")))*1000)""")
          .as(SurveyFields.DATE),
        currentA0UserDf(SurveyFields.BUSINESS_UNIT),
        currentA0UserDf(SurveyFields.MARKET_CENTER_ID),
        when(
          surveyDf(SurveyFields.TEAM_ID).isNull.or(surveyDf(SurveyFields.TEAM_ID).equalTo("")),
          currentA0UserDf(SurveyFields.TEAM_ID)
        ).otherwise(surveyDf(SurveyFields.TEAM_ID)).as(SurveyFields.TEAM_ID),
        currentA0UserDf(SurveyFields.USER_TYPE),
        surveyDf(SurveyFields.AGENT_ID),
        surveyDf(SurveyFields.SOURCE),
        surveyDf(SurveyFields.CID),
        surveyDf(SurveyFields.TEMPLATE_ID),
        surveyDf(SurveyFields.CHANNEL),
        surveyDf(SurveyFields.SURVEY_STATUS),
        surveyDf(SurveyFields.SURVEY_ID)
      )

    resultDf.na
      .fill("unknown")
      .na
      .replace(
        Seq(
          SurveyFields.BUSINESS_UNIT,
          SurveyFields.MARKET_CENTER_ID,
          SurveyFields.TEAM_ID,
          SurveyFields.USER_TYPE,
          SurveyFields.AGENT_ID,
          SurveyFields.SOURCE,
          SurveyFields.CID,
          SurveyFields.TEMPLATE_ID,
          SurveyFields.CHANNEL,
          SurveyFields.SURVEY_STATUS,
          SurveyFields.SURVEY_ID
        ),
        Map("" -> "unknown")
      )
  }

  private def explodeCids(rawSurveyDf: DataFrame): DataFrame = {
    rawSurveyDf
      .withColumn(
        SurveyFields.CIDS,
        regexp_replace(col(SurveyFields.CIDS), "[\\[\\'\\'\\]]", "")
      )
      .withColumn(SurveyFields.CIDS, split(col(SurveyFields.CIDS), ","))
      .withColumn(SurveyFields.CID, explode(col(SurveyFields.CIDS)))
      .drop(SurveyFields.CIDS)
  }

  private def buildA0Df(previousA0Df: Option[DataFrame], dailySurveyDf: DataFrame, config: Config): DataFrame = {
    previousA0Df match {
      case Some(previousA0Df) =>
        previousA0Df
          .unionByName(dailySurveyDf, allowMissingColumns = true)
          .withColumn(SurveyFields.DATE, lit(config.getDailyReportTime))
          .sort(col(SurveyFields.SURVEY_ID).desc)
          .dropDuplicates(SurveyFields.SURVEY_ID)
          .persist(StorageLevel.MEMORY_AND_DISK_2)
      case _ => dailySurveyDf
    }
  }

  private def buildAnDf(previousA0Df: Option[DataFrame], dailySurveyDf: DataFrame): DataFrame = {
    previousA0Df match {
      case Some(previousA0Df) =>
        dailySurveyDf
          .join(
            previousA0Df,
            dailySurveyDf(SurveyFields.SURVEY_ID) === previousA0Df(SurveyFields.SURVEY_ID),
            "leftanti"
          )
      case _ => dailySurveyDf
    }
  }
}
