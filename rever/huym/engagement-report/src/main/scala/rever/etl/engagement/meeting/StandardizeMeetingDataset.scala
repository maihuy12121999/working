package rever.etl.engagement.meeting

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import rever.etl.engagement.domain.MeetingFields
import rever.etl.engagement.meeting.reader.{MeetingReader, UserHistoricalReader}
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.Table
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.domain.SparkSessionImplicits._

class StandardizeMeetingDataset extends FlowMixin {

  @Table("meeting_standardized_df")
  def build(
      @Table(
        name = "historical.engagement_historical_1",
        reader = classOf[MeetingReader]
      ) dailyDf: DataFrame,
      @Table(
        name = "current_a0_df",
        reader = classOf[UserHistoricalReader]
      ) currentUserA0Df: DataFrame,
      config: Config
  ): DataFrame = {
    dailyDf.printSchema()
    val distinctDailyDf = dailyDf.dropDuplicateCols(Seq(MeetingFields.MEETING_ID), col(MeetingFields.TIMESTAMP).desc)
    val df = explodeCIds(distinctDailyDf)
    val normalizedDf = enhanceAndNormalizeDf(df, currentUserA0Df)
    normalizedDf
  }

  private def explodeCIds(dailyDf: DataFrame): DataFrame = {
    dailyDf
      .withColumn(MeetingFields.CIDS, MeetingHelper.addCIdsColumnUdf(col(MeetingFields.CID)))
      .withColumn(MeetingFields.CID, explode(col(MeetingFields.CIDS)))
      .drop(MeetingFields.CIDS)
  }

  private def enhanceAndNormalizeDf(df: DataFrame, currentUserA0Df: DataFrame): DataFrame = {

    val resultDf = df
      .joinWith(currentUserA0Df, df(MeetingFields.AGENT_ID) === currentUserA0Df("username"), "left")
      .select(
        expr(s"""(BIGINT(to_timestamp(to_date(timestamp_millis(_1.${MeetingFields.TIMESTAMP}),"dd/MM/yyyy")))*1000)""")
          .as(
            MeetingFields.DATE
          ),
        col(s"_2.${MeetingFields.BUSINESS_UNIT}"),
        when(
          col(s"_1.${MeetingFields.MARKET_CENTER_ID}") === "" || col(s"_1.${MeetingFields.MARKET_CENTER_ID}").isNull
            || col(s"_1.${MeetingFields.MARKET_CENTER_ID}") === "unknown",
          col(s"_2.${MeetingFields.MARKET_CENTER_ID}")
        ).otherwise(col(s"_1.${MeetingFields.MARKET_CENTER_ID}")).as(MeetingFields.MARKET_CENTER_ID),
        when(
          col(s"_1.${MeetingFields.TEAM_ID}") === "" || col(s"_1.${MeetingFields.TEAM_ID}").isNull
            || col(s"_1.${MeetingFields.TEAM_ID}") === "unknown",
          col(s"_2.${MeetingFields.TEAM_ID}")
        ).otherwise(col(s"_1.${MeetingFields.TEAM_ID}")).as(MeetingFields.TEAM_ID),
        col(s"_2.${MeetingFields.USER_TYPE}"),
        col(s"_1.${MeetingFields.AGENT_ID}"),
        col(s"_1.${MeetingFields.SOURCE}"),
        col(s"_1.${MeetingFields.CID}"),
        when(
          col(s"_1.${MeetingFields.STATUS}") === "" || col(s"_1.${MeetingFields.STATUS}").isNull
            || col(s"_1.${MeetingFields.STATUS}") === "unknown",
          col(s"_2.${MeetingFields.STATUS}")
        ).otherwise(col(s"_1.${MeetingFields.STATUS}")).as(MeetingFields.STATUS),
        col(s"_1.${MeetingFields.MEETING_ID}"),
        col(s"_1.${MeetingFields.DURATION}")
      )

    MeetingHelper.standardizeStringCols(
      resultDf,
      Seq(
        MeetingFields.BUSINESS_UNIT,
        MeetingFields.MARKET_CENTER_ID,
        MeetingFields.TEAM_ID,
        MeetingFields.USER_TYPE,
        MeetingFields.AGENT_ID,
        MeetingFields.SOURCE,
        MeetingFields.STATUS,
        MeetingFields.CID,
        MeetingFields.MEETING_ID
      ),
      "unknown"
    )
  }
}
