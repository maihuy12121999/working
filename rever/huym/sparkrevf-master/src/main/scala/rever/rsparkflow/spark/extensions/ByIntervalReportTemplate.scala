package rever.rsparkflow.spark.extensions

import org.apache.spark.api.java.StorageLevels
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{Column, DataFrame, Row, SparkSession}
import rever.rsparkflow.spark.FlowMixin
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.domain.IntervalTypes
import rever.rsparkflow.spark.utils.TimestampUtils

import scala.concurrent.duration.DurationInt

/**
  * @author anhlt
  */

object ByIntervalReportTemplate {
  final val DATA_TYPE = "data_type"
  final val DATE = "date"
}

trait ByIntervalReportTemplate extends FlowMixin with CommonExtension {

  def calcByInterval(
      jobId: String,
      df: DataFrame,
      reportTime: Long,
      dimensions: Seq[String],
      metricColMap: Map[Column, String],
      generator: Row => Seq[Row],
      config: Config,
      namePrefix: Option[String] = None,
      schema: Option[StructType] = None,
      exportA1Dataset: Boolean = true
  ): DataFrame = {
    val dailyDf = df
      .withColumn(ByIntervalReportTemplate.DATE, lit(reportTime))
      .persist(StorageLevels.MEMORY_AND_DISK_2)

    val weeklyDf = buildWeeklyDf(jobId, dailyDf, config, namePrefix, schema)
    val monthlyDf = buildMonthlyDf(jobId, dailyDf, config, namePrefix, schema)
    val quarterlyDf = buildQuarterlyDf(jobId, monthlyDf, config, namePrefix, schema)

    if (exportA1Dataset) {
      exportA1DfToS3(jobId, reportTime, dailyDf, config, namePrefix)
    }

    val dailyResultDf = execResultDf(IntervalTypes.DAILY, dailyDf, dimensions, metricColMap, generator, config)
    val weeklyResultDf = execResultDf(IntervalTypes.WEEKLY, weeklyDf, dimensions, metricColMap, generator, config)
    val monthlyResultDf = execResultDf(IntervalTypes.MONTHLY, monthlyDf, dimensions, metricColMap, generator, config)
    val quarterlyResultDf = execResultDf(
      IntervalTypes.QUARTERLY,
      quarterlyDf,
      dimensions,
      metricColMap,
      generator,
      config
    )

    val resultDf = dailyResultDf
      .unionByName(weeklyResultDf, allowMissingColumns = true)
      .unionByName(monthlyResultDf, allowMissingColumns = true)
      .unionByName(quarterlyResultDf, allowMissingColumns = true)

    resultDf
  }

  def calcAnByInterval(
      jobId: String,
      anDf: DataFrame,
      reportTime: Long,
      dimensions: Seq[String],
      metricColMap: Map[Column, String],
      generator: Row => Seq[Row],
      config: Config,
      namePrefix: Option[String] = None,
      schema: Option[StructType] = None,
      exportAnDataset: Boolean = true,
      persistDataset: Boolean = false
  ): DataFrame = {
    val dailyAnDf = anDf
      .withColumn(ByIntervalReportTemplate.DATE, lit(reportTime))
      .persist(StorageLevels.MEMORY_AND_DISK_2)

    val weeklyDf = buildAnWeeklyDf(jobId, dailyAnDf, config, namePrefix, schema) match {
      case df if persistDataset => df.persist(StorageLevels.MEMORY_AND_DISK_2)
      case df                   => df
    }
    val monthlyDf = buildAnMonthlyDf(jobId, dailyAnDf, config, namePrefix, schema) match {
      case df if persistDataset => df.persist(StorageLevels.MEMORY_AND_DISK_2)
      case df                   => df
    }
    val quarterlyDf = buildAnQuarterlyDf(jobId, monthlyDf, config, namePrefix, schema)

    if (exportAnDataset) {
      exportAnDfToS3(jobId, reportTime, dailyAnDf, config, namePrefix)
    }

    val dailyResultDf = execResultDf(IntervalTypes.DAILY, dailyAnDf, dimensions, metricColMap, generator, config)
    val weeklyResultDf = execResultDf(IntervalTypes.WEEKLY, weeklyDf, dimensions, metricColMap, generator, config)
    val monthlyResultDf = execResultDf(IntervalTypes.MONTHLY, monthlyDf, dimensions, metricColMap, generator, config)
    val quarterlyResultDf = execResultDf(
      IntervalTypes.QUARTERLY,
      quarterlyDf,
      dimensions,
      metricColMap,
      generator,
      config
    )

    val resultDf = dailyResultDf
      .unionByName(weeklyResultDf, allowMissingColumns = true)
      .unionByName(monthlyResultDf, allowMissingColumns = true)
      .unionByName(quarterlyResultDf, allowMissingColumns = true)

    resultDf
  }

  def calcA0ByInterval(
      a0Df: DataFrame,
      reportTime: Long,
      dimensions: Seq[String],
      metricColMap: Map[Column, String],
      generator: Row => Seq[Row],
      config: Config,
      persistDataset: Boolean = false
  ): DataFrame = {
    val dailyA0Df = a0Df.withColumn(ByIntervalReportTemplate.DATE, lit(reportTime))

    val dailyResultDf = execResultDf(IntervalTypes.DAILY, dailyA0Df, dimensions, metricColMap, generator, config)
      .persist(StorageLevels.MEMORY_AND_DISK_2)
    val weeklyResultDf = if (needExecuteInterval(IntervalTypes.WEEKLY, config)) {
      dailyResultDf
        .withColumn(ByIntervalReportTemplate.DATA_TYPE, lit(IntervalTypes.WEEKLY))
        .withColumn(ByIntervalReportTemplate.DATE, lit(TimestampUtils.asStartOfWeek(reportTime)))
    } else {
      SparkSession.active.emptyDataset(RowEncoder(dailyResultDf.schema))
    }

    val monthlyResultDf = if (needExecuteInterval(IntervalTypes.MONTHLY, config)) {
      dailyResultDf
        .withColumn(ByIntervalReportTemplate.DATA_TYPE, lit(IntervalTypes.MONTHLY))
        .withColumn(ByIntervalReportTemplate.DATE, lit(TimestampUtils.asStartOfMonth(reportTime)))
    } else {
      SparkSession.active.emptyDataset(RowEncoder(dailyResultDf.schema))
    }

    val quarterlyResultDf = if (needExecuteInterval(IntervalTypes.QUARTERLY, config)) {
      dailyResultDf
        .withColumn(ByIntervalReportTemplate.DATA_TYPE, lit(IntervalTypes.QUARTERLY))
        .withColumn(ByIntervalReportTemplate.DATE, lit(TimestampUtils.asStartOfQuarter(reportTime)))
    } else {
      SparkSession.active.emptyDataset(RowEncoder(dailyResultDf.schema))
    }

    val resultDf = dailyResultDf
      .unionByName(weeklyResultDf, allowMissingColumns = true)
      .unionByName(monthlyResultDf, allowMissingColumns = true)
      .unionByName(quarterlyResultDf, allowMissingColumns = true)

    resultDf
  }

  private def execResultDf(
      intervalType: String,
      df: DataFrame,
      dimensions: Seq[String],
      metricColMap: Map[Column, String],
      generator: Row => Seq[Row],
      config: Config
  ): DataFrame = {

    val aggCols = metricColMap.map {
      case (col, targetColName) =>
        col.as(targetColName)
    }.toSeq

    val resultDf = if (needExecuteInterval(intervalType, config)) {
      df.flatMap(generator)(RowEncoder(df.schema))
    } else {
      println(
        s"Create empty dataset for ${intervalType} at date= ${TimestampUtils.format(config.getDailyReportTime)}"
      )
      SparkSession.active.emptyDataset(RowEncoder(df.schema))
    }

    resultDf
      .withColumn(ByIntervalReportTemplate.DATA_TYPE, lit(intervalType))
      .groupBy((Seq(ByIntervalReportTemplate.DATA_TYPE, ByIntervalReportTemplate.DATE) ++ dimensions).map(col): _*)
      .agg(
        aggCols.head,
        aggCols.tail: _*
      )

  }

  private def needExecuteInterval(intervalType: String, config: Config): Boolean = {

    val yesterday = System.currentTimeMillis() - 1.days.toMillis

    val reportTime = config.getDailyReportTime

    intervalType match {
      case IntervalTypes.DAILY => true
      case IntervalTypes.WEEKLY =>
        TimestampUtils.isEndOfWeek(reportTime) || TimestampUtils.isSameDay(reportTime, yesterday)
      case IntervalTypes.MONTHLY =>
        TimestampUtils.isEndOfMonth(reportTime) || TimestampUtils.isSameDay(reportTime, yesterday)
      case IntervalTypes.QUARTERLY =>
        TimestampUtils.isEndOfQuarter(reportTime) || TimestampUtils.isSameDay(reportTime, yesterday)
      case _ => false
    }

  }

}
