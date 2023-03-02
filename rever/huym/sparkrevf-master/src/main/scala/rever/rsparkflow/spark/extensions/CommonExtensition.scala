package rever.rsparkflow.spark.extensions

import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame}
import rever.rsparkflow.spark.FlowMixin
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.utils.TimestampUtils

import scala.concurrent.duration.DurationInt

private[extensions] trait CommonExtension extends FlowMixin {

  protected def toJoinCol(leftDf: DataFrame, rightDf: DataFrame, fields: Seq[String]): Column = {
    val firstField = fields.head

    fields.tail.foldLeft(leftDf(firstField) === rightDf(firstField))((col, field) => {
      col && (leftDf(field) === rightDf(field))
    })
  }

  protected def buildAnWeeklyDf(
      jobId: String,
      dailyDf: DataFrame,
      config: Config,
      namePrefix: Option[String] = None,
      schema: Option[StructType] = None
  ): DataFrame = {
    val reportTime = config.getDailyReportTime

    val (startWeek, _) = TimestampUtils.toStartEndOfWeek(reportTime)

    val df = loadAllAnDfFromS3(config, jobId, startWeek, reportTime - 1.days.toMillis, namePrefix, schema) match {
      case Some(prevDf) => prevDf.unionByName(dailyDf, allowMissingColumns = true)
      case None         => dailyDf
    }

    df.withColumn("date", lit(startWeek))
  }

  protected def buildAnMonthlyDf(
      jobId: String,
      dailyDf: DataFrame,
      config: Config,
      namePrefix: Option[String] = None,
      schema: Option[StructType] = None
  ): DataFrame = {
    val reportTime = config.getDailyReportTime

    val (startMonth, _) = TimestampUtils.toStartEndOfMonth(reportTime)

    val df = loadAllAnDfFromS3(config, jobId, startMonth, reportTime - 1.days.toMillis, namePrefix, schema) match {
      case Some(prevDf) =>
        prevDf.unionByName(dailyDf, allowMissingColumns = true)
      case None => dailyDf
    }

    df.withColumn("date", lit(startMonth))
  }

  protected def buildAnQuarterlyDf(
      jobId: String,
      monthlyDf: DataFrame,
      config: Config,
      namePrefix: Option[String] = None,
      schema: Option[StructType] = None
  ): DataFrame = {
    val reportTime = config.getDailyReportTime

    val (startMonth, _) = TimestampUtils.toStartEndOfMonth(reportTime)
    val (startQuarter, _) = TimestampUtils.toStartEndOfQuarter(reportTime)

    val df = loadAllAnDfFromS3(config, jobId, startQuarter, startMonth - 1.days.toMillis, namePrefix, schema) match {
      case Some(prevDf) =>
        prevDf.unionByName(monthlyDf, allowMissingColumns = true)
      case None => monthlyDf
    }

    df.withColumn("date", lit(startQuarter))
  }

  protected def buildWeeklyDf(
      jobId: String,
      dailyDf: DataFrame,
      config: Config,
      namePrefix: Option[String] = None,
      schema: Option[StructType] = None
  ): DataFrame = {
    val reportTime = config.getDailyReportTime

    val (startWeek, _) = TimestampUtils.toStartEndOfWeek(reportTime)

    val df = loadAllA1DfFromS3(config, jobId, startWeek, reportTime - 1.days.toMillis, namePrefix, schema) match {
      case Some(prevDf) =>
        prevDf.unionByName(dailyDf, allowMissingColumns = true)
      case None => dailyDf
    }

    df.withColumn("date", lit(startWeek))
  }

  protected def buildMonthlyDf(
      jobId: String,
      dailyDf: DataFrame,
      config: Config,
      namePrefix: Option[String] = None,
      schema: Option[StructType] = None
  ): DataFrame = {
    val reportTime = config.getDailyReportTime

    val (startMonth, _) = TimestampUtils.toStartEndOfMonth(reportTime)

    val df = loadAllA1DfFromS3(config, jobId, startMonth, reportTime - 1.days.toMillis, namePrefix, schema) match {
      case Some(prevDf) => prevDf.unionByName(dailyDf, allowMissingColumns = true)
      case None         => dailyDf
    }

    df.withColumn("date", lit(startMonth))
  }

  protected def buildQuarterlyDf(
      jobId: String,
      monthlyDf: DataFrame,
      config: Config,
      namePrefix: Option[String] = None,
      schema: Option[StructType] = None
  ): DataFrame = {
    val reportTime = config.getDailyReportTime

    val (startMonth, _) = TimestampUtils.toStartEndOfMonth(reportTime)
    val (startQuarter, _) = TimestampUtils.toStartEndOfQuarter(reportTime)

    val df = loadAllA1DfFromS3(config, jobId, startQuarter, startMonth - 1.days.toMillis, namePrefix, schema) match {
      case Some(prevDf) =>
        prevDf.unionByName(monthlyDf, allowMissingColumns = true)
      case None => monthlyDf
    }

    df.withColumn("date", lit(startQuarter))
  }
}
