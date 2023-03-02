package rever.etl.engagement.meeting

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import rever.etl.engagement.domain.MeetingFields
import rever.etl.engagement.meeting.writer.MeetingWriter
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.{Output, Table}
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.domain.IntervalTypes
import rever.etl.rsparkflow.utils.TimestampUtils

import scala.concurrent.duration.DurationInt

class MeetingReport extends FlowMixin {

  @Output(writer = classOf[MeetingWriter])
  @Table("meeting_report_df")
  def build(@Table(name = "meeting_standardized_df") dailyMeetingDf: DataFrame, config: Config): DataFrame = {

    val previousA0Df = getPreviousA0DfFromS3(config.getJobId, config.getDailyReportTime, config).map(
      _.persist(StorageLevel.MEMORY_AND_DISK_2)
    )
    val anDf = buildAnDf(dailyMeetingDf, previousA0Df).persist(StorageLevel.MEMORY_AND_DISK_2)

    val a0Df = buildA0Df(previousA0Df, anDf)

    exportAnDfToS3(config.getJobId, config.getDailyReportTime, anDf, config)
    exportA0DfToS3(config.getJobId, config.getDailyReportTime, a0Df, config)

    val weeklyDf = buildWeeklyDf(anDf, config)
    val monthlyDf = buildMonthlyDf(anDf, weeklyDf, config)
    val quarterlyDf = buildQuarterlyDf(monthlyDf, config)
    val resultDf = anDf
      .withColumn(MeetingFields.INTERVAL, lit(IntervalTypes.DAILY))
      .unionByName(weeklyDf)
      .unionByName(monthlyDf)
      .unionByName(quarterlyDf)
      .select(
        MeetingFields.DATE,
        MeetingFields.INTERVAL,
        MeetingFields.BUSINESS_UNIT,
        MeetingFields.MARKET_CENTER_ID,
        MeetingFields.TEAM_ID,
        MeetingFields.USER_TYPE,
        MeetingFields.AGENT_ID,
        MeetingFields.SOURCE,
        MeetingFields.CID,
        MeetingFields.STATUS,
        MeetingFields.MEETING_ID,
        MeetingFields.DURATION
      )

    resultDf
      .flatMap(row => MeetingHelper.generateMeetingRow(row))(RowEncoder(MeetingHelper.definedSchema))
      .groupBy(
        col(MeetingFields.DATE),
        col(MeetingFields.INTERVAL),
        col(MeetingFields.BUSINESS_UNIT),
        col(MeetingFields.MARKET_CENTER_ID),
        col(MeetingFields.TEAM_ID),
        col(MeetingFields.USER_TYPE),
        col(MeetingFields.AGENT_ID),
        col(MeetingFields.SOURCE),
        col(MeetingFields.CID),
        col(MeetingFields.STATUS)
      )
      .agg(
        countDistinct(MeetingFields.MEETING_ID).as(MeetingFields.TOTAL_MEETING),
        sum(MeetingFields.DURATION).as(MeetingFields.TOTAL_DURATION),
        min(MeetingFields.DURATION).as(MeetingFields.MIN_DURATION),
        max(MeetingFields.DURATION).as(MeetingFields.MAX_DURATION)
      )
  }
  private def buildAnDf(dailyMeetingDf: DataFrame, previousA0Df: Option[DataFrame]): DataFrame = {
    previousA0Df match {
      case None => dailyMeetingDf
      case Some(previousA0Df) => {
        dailyMeetingDf
          .join(
            previousA0Df,
            dailyMeetingDf(MeetingFields.MEETING_ID) === previousA0Df(MeetingFields.MEETING_ID),
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
      .withColumn(MeetingFields.INTERVAL, lit(IntervalTypes.WEEKLY))
      .withColumn(MeetingFields.DATE, lit(startOfWeekTimeStamp))
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
      .withColumn(MeetingFields.INTERVAL, lit(IntervalTypes.MONTHLY))
      .withColumn(MeetingFields.DATE, lit(startOfMonthTimeStamp))
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
      .withColumn(MeetingFields.INTERVAL, lit(IntervalTypes.QUARTERLY))
      .withColumn(MeetingFields.DATE, lit(startOfQuarterTimeStamp))
  }
}
