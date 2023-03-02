package rever.etl.engagement.note

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, countDistinct, lit}
import org.apache.spark.storage.StorageLevel
import rever.etl.engagement.domain.NoteFields
import rever.etl.engagement.note.writer.NoteWriter
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.{Output, Table}
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.domain.IntervalTypes
import rever.etl.rsparkflow.utils.TimestampUtils

import scala.concurrent.duration.DurationInt
class NoteReport extends FlowMixin {
  @Output(writer = classOf[NoteWriter])
  @Table("note_report_df")
  def build(@Table(name = "note_standardized_df") dailyNoteDf: DataFrame, config: Config): DataFrame = {
    val previousA0Df = getPreviousA0DfFromS3(config.getJobId, config.getDailyReportTime, config).map(
      _.persist(StorageLevel.MEMORY_AND_DISK_2)
    )
    val anDf = buildAnDf(dailyNoteDf, previousA0Df).persist(StorageLevel.MEMORY_AND_DISK_2)
    exportAnDfToS3(config.getJobId, config.getDailyReportTime, anDf, config)
    val a0Df = buildA0Df(previousA0Df, anDf)
    exportA0DfToS3(config.getJobId, config.getDailyReportTime, a0Df, config)
    val weeklyDf = buildWeeklyDf(anDf, config)
    val monthlyDf = buildMonthlyDf(anDf, weeklyDf, config)
    val quarterlyDf = buildQuarterlyDf(monthlyDf, config)
    val resultDf = anDf
      .withColumn(NoteFields.INTERVAL, lit(IntervalTypes.DAILY))
      .unionByName(weeklyDf)
      .unionByName(monthlyDf)
      .unionByName(quarterlyDf)
      .select(
        NoteFields.DATE,
        NoteFields.INTERVAL,
        NoteFields.BUSINESS_UNIT,
        NoteFields.MARKET_CENTER_ID,
        NoteFields.TEAM_ID,
        NoteFields.USER_TYPE,
        NoteFields.AGENT_ID,
        NoteFields.SOURCE,
        NoteFields.CID,
        NoteFields.STATUS,
        NoteFields.NOTE_ID
      )

    resultDf
      .flatMap(row => NoteHelper.generateProductRow(row))(RowEncoder(NoteHelper.definedSchema))
      .groupBy(
        col(NoteFields.DATE),
        col(NoteFields.INTERVAL),
        col(NoteFields.BUSINESS_UNIT),
        col(NoteFields.MARKET_CENTER_ID),
        col(NoteFields.TEAM_ID),
        col(NoteFields.USER_TYPE),
        col(NoteFields.AGENT_ID),
        col(NoteFields.SOURCE),
        col(NoteFields.CID),
        col(NoteFields.STATUS)
      )
      .agg(
        countDistinct(NoteFields.NOTE_ID).as(NoteFields.TOTAL_NOTE)
      )
  }
  private def buildAnDf(dailyMeetingDf: DataFrame, previousA0Df: Option[DataFrame]): DataFrame = {
    previousA0Df match {
      case None => dailyMeetingDf
      case Some(previousA0Df) => {
        dailyMeetingDf
          .join(
            previousA0Df,
            dailyMeetingDf(NoteFields.NOTE_ID) === previousA0Df(NoteFields.NOTE_ID),
            "leftanti"
          )
      }
    }
  }

  private def buildA0Df(previousA0Df: Option[DataFrame], anDf: DataFrame): DataFrame = {
    previousA0Df match {
      case None => anDf
      case Some(previousA0Df) =>
        previousA0Df
          .unionByName(anDf, allowMissingColumns = true)
    }
  }
  private def buildWeeklyDf(anDf: DataFrame, config: Config): DataFrame = {
    val reportTime = config.getDailyReportTime
    val startOfWeekTimeStamp = TimestampUtils.asStartOfWeek(reportTime)
    val df = loadAllAnDfFromS3(config, config.getJobId, startOfWeekTimeStamp, reportTime - 1.days.toMillis)
    val resultDf = df match {
      case None     => anDf
      case Some(df) => df.unionByName(anDf, allowMissingColumns = true)
    }
    resultDf
      .withColumn(NoteFields.INTERVAL, lit(IntervalTypes.WEEKLY))
      .withColumn(NoteFields.DATE, lit(startOfWeekTimeStamp))
  }
  private def buildMonthlyDf(anDf: DataFrame, weeklyDf: DataFrame, config: Config): DataFrame = {
    val reportTime = config.getDailyReportTime
    val startOfMonthTimeStamp = TimestampUtils.asStartOfMonth(reportTime)
    val startOfWeekTimeStamp = TimestampUtils.asStartOfWeek(reportTime)
    val resultDf =
      if (startOfWeekTimeStamp < startOfMonthTimeStamp) {
        val df = loadAllAnDfFromS3(config, config.getJobId, startOfMonthTimeStamp, reportTime - 1.days.toMillis)
        df match {
          case None => anDf
          case Some(df) =>
            df.unionByName(anDf, allowMissingColumns = true)
        }
      } else {
        val df =
          loadAllAnDfFromS3(config, config.getJobId, startOfMonthTimeStamp, startOfWeekTimeStamp - 1.days.toMillis)
        df match {
          case None => weeklyDf
          case Some(df) =>
            df.unionByName(weeklyDf, allowMissingColumns = true)
        }
      }
    resultDf
      .withColumn(NoteFields.INTERVAL, lit(IntervalTypes.MONTHLY))
      .withColumn(NoteFields.DATE, lit(startOfMonthTimeStamp))
  }
  private def buildQuarterlyDf(monthlyDf: DataFrame, config: Config): DataFrame = {
    val reportTime = config.getDailyReportTime
    val startOfQuarterTimeStamp = TimestampUtils.asStartOfQuarter(reportTime)
    val startOfMonthTimeStamp = TimestampUtils.asStartOfMonth(reportTime)
    val df: Option[DataFrame] =
      loadAllAnDfFromS3(config, config.getJobId, startOfQuarterTimeStamp, startOfMonthTimeStamp - 1.days.toMillis)
    val resultDf = df match {
      case None     => monthlyDf
      case Some(df) => df.unionByName(monthlyDf, allowMissingColumns = true)
    }
    resultDf
      .withColumn(NoteFields.INTERVAL, lit(IntervalTypes.QUARTERLY))
      .withColumn(NoteFields.DATE, lit(startOfQuarterTimeStamp))
  }
}
