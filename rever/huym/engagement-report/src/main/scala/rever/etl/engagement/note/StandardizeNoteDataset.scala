package rever.etl.engagement.note

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import rever.etl.engagement.domain.NoteFields
import rever.etl.engagement.note.reader.{NoteReader, UserHistoricalReader}
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.Table
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.domain.SparkSessionImplicits._
class StandardizeNoteDataset extends FlowMixin {
  @Table("note_standardized_df")
  def build(
             @Table(
               name = "historical.engagement_historical_1",
               reader = classOf[NoteReader]
             ) dailyDf: DataFrame,
             @Table(
               name = "current_a0_df",
               reader = classOf[UserHistoricalReader]
             ) currentUserA0Df: DataFrame,
             config: Config
           ): DataFrame = {
    val distinctDailyDf = dailyDf.dropDuplicateCols(Seq(NoteFields.NOTE_ID), col(NoteFields.TIMESTAMP).desc)
    val df = explodeCIds(distinctDailyDf)
    val normalizedDf = enhanceAndNormalizeDf(df, currentUserA0Df)
    normalizedDf
  }

  private def explodeCIds(dailyDf: DataFrame): DataFrame = {
    dailyDf
      .withColumn(NoteFields.CIDS, regexp_replace(col(NoteFields.CIDS), "[\\[\\'\\]]", ""))
      .withColumn(NoteFields.CIDS, split(col(NoteFields.CIDS), ","))
      .withColumn(NoteFields.CID, explode(col(NoteFields.CIDS)))
      .drop(NoteFields.CIDS)
  }

  private def enhanceAndNormalizeDf(df: DataFrame, currentUserA0Df: DataFrame): DataFrame = {
    df
      .joinWith(currentUserA0Df, df(NoteFields.AGENT_ID) === currentUserA0Df("username"), "left")
      .select(
        expr(s"""(BIGINT(to_timestamp(to_date(timestamp_millis(_1.${NoteFields.TIMESTAMP}),"dd/MM/yyyy")))*1000)""")
          .as(
            NoteFields.DATE
          ),
        col(s"_2.${NoteFields.BUSINESS_UNIT}"),
        when(
          col(s"_1.${NoteFields.MARKET_CENTER_ID}") === "" || col(s"_1.${NoteFields.MARKET_CENTER_ID}").isNull
            || col(s"_1.${NoteFields.MARKET_CENTER_ID}") === "unknown",
          col(s"_2.${NoteFields.MARKET_CENTER_ID}")
        ).otherwise(col(s"_1.${NoteFields.MARKET_CENTER_ID}")).as(NoteFields.MARKET_CENTER_ID),
        when(
          col(s"_1.${NoteFields.TEAM_ID}") === "" || col(s"_1.${NoteFields.TEAM_ID}").isNull
            || col(s"_1.${NoteFields.TEAM_ID}") === "unknown",
          col(s"_2.${NoteFields.TEAM_ID}")
        ).otherwise(col(s"_1.${NoteFields.TEAM_ID}")).as(NoteFields.TEAM_ID),
        col(s"_2.${NoteFields.USER_TYPE}"),
        col(s"_1.${NoteFields.AGENT_ID}"),
        col(s"_1.${NoteFields.SOURCE}"),
        col(s"_1.${NoteFields.CID}"),
        when(
          col(s"_1.${NoteFields.STATUS}") === "" || col(s"_1.${NoteFields.STATUS}").isNull
            || col(s"_1.${NoteFields.TEAM_ID}") === "unknown",
          col(s"_2.${NoteFields.STATUS}")
        ).otherwise(col(s"_1.${NoteFields.STATUS}")).as(NoteFields.STATUS),
        col(s"_1.${NoteFields.NOTE_ID}")
      )
      .na
      .fill("unknown")
      .na
      .replace(
        Seq(
          NoteFields.BUSINESS_UNIT,
          NoteFields.MARKET_CENTER_ID,
          NoteFields.TEAM_ID,
          NoteFields.USER_TYPE,
          NoteFields.AGENT_ID,
          NoteFields.SOURCE,
          NoteFields.CID,
          NoteFields.NOTE_ID
        ),
        Map("" -> "unknown")
      )
  }
}
