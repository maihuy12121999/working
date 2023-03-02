package rever.rsparkflow.spark.extensions

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.storage.StorageLevel
import rever.rsparkflow.spark.FlowMixin
import rever.rsparkflow.spark.api.configuration.Config

import scala.concurrent.duration.DurationInt

/**
  * @author anhlt
  */
object ActiveReportTemplate {
  final val DATE = "date"

  final val TOTAL_USERS = "total_user"
  final val TOTAL_ACTIONS = "total_actions"
  final val A1 = "a1"
  final val TOTAL_A1_ACTIONS = "total_a1_actions"
  final val A7 = "a7"
  final val TOTAL_A7_ACTIONS = "total_a7_actions"
  final val A30 = "a30"
  final val TOTAL_A30_ACTIONS = "total_a30_actions"
  final val AN = "an"
  final val TOTAL_AN_ACTIONS = "total_an_actions"
  final val A0 = "a0"
  final val TOTAL_A0_ACTIONS = "total_a0_actions"
}

trait ActiveReportTemplate extends FlowMixin with CommonExtension {

  def activeUsers(
      jobId: String,
      df: DataFrame,
      date: Long,
      dimensions: Seq[String],
      uniqueFields: Seq[String],
      config: Config,
      namePrefix: Option[String] = None,
      schema: Option[StructType] = None
  )(generator: Row => Seq[Row]): DataFrame = {
    val a1Df = df
      .withColumn(ActiveReportTemplate.DATE, lit(date))
      .groupBy((Seq(ActiveReportTemplate.DATE) ++ dimensions ++ uniqueFields).map(col): _*)
      .agg(count(col(uniqueFields.head)).as(ActiveReportTemplate.TOTAL_ACTIONS))
      .persist(StorageLevel.MEMORY_AND_DISK_2)

    exportA1DfToS3(jobId, date, a1Df, config, namePrefix)

    val previousA0Df: Option[DataFrame] = getPreviousA0DfFromS3(jobId, date, config, namePrefix)

    val a0Df = previousA0Df match {
      case Some(previousA0Df) =>
        previousA0Df
          .unionByName(a1Df, allowMissingColumns = true)
          .withColumn(ActiveReportTemplate.DATE, lit(date))
          .groupBy((Seq(ActiveReportTemplate.DATE) ++ dimensions ++ uniqueFields).map(col): _*)
          .agg(sum(col(ActiveReportTemplate.TOTAL_ACTIONS)).as(ActiveReportTemplate.TOTAL_ACTIONS))
          .persist(StorageLevel.MEMORY_AND_DISK_2)
      case _ => a1Df
    }

    val anDf = previousA0Df match {
      case Some(previousA0Df) =>
        a1Df.join(
          previousA0Df,
          toJoinCol(a1Df, previousA0Df, uniqueFields),
          "leftanti"
        )
      case _ => a1Df
    }

    exportAnDfToS3(jobId, date, anDf, config, namePrefix)
    exportA0DfToS3(jobId, date, a0Df, config, namePrefix)

    val a7Df = buildA7Df(jobId, date, a1Df, config, namePrefix, schema)
    val a30Df = buildA30Df(jobId, date, a7Df, config, namePrefix, schema)

    activeUserResultDf(date, a1Df, a7Df, a30Df, anDf, a0Df, dimensions, uniqueFields, generator)

  }

  private def activeUserResultDf(
      date: Long,
      a1Df: DataFrame,
      a7Df: DataFrame,
      a30Df: DataFrame,
      anDf: DataFrame,
      a0Df: DataFrame,
      dimensions: Seq[String],
      uniqueFields: Seq[String],
      generator: Row => Seq[Row]
  ): DataFrame = {

    val a1ResultDf = executeResultDf(
      date,
      a1Df,
      dimensions,
      uniqueFields,
      generator,
      ActiveReportTemplate.A1,
      ActiveReportTemplate.TOTAL_A1_ACTIONS
    )

    val a7ResultDf = executeResultDf(
      date,
      a7Df,
      dimensions,
      uniqueFields,
      generator,
      ActiveReportTemplate.A7,
      ActiveReportTemplate.TOTAL_A7_ACTIONS
    )

    val a30ResultDf = executeResultDf(
      date,
      a30Df,
      dimensions,
      uniqueFields,
      generator,
      ActiveReportTemplate.A30,
      ActiveReportTemplate.TOTAL_A30_ACTIONS
    )

    val anResultDf = executeResultDf(
      date,
      anDf,
      dimensions,
      uniqueFields,
      generator,
      ActiveReportTemplate.AN,
      ActiveReportTemplate.TOTAL_AN_ACTIONS
    )

    val a0ResultDf = executeResultDf(
      date,
      a0Df,
      dimensions,
      uniqueFields,
      generator,
      ActiveReportTemplate.A0,
      ActiveReportTemplate.TOTAL_A0_ACTIONS
    )

    val joinCols: Seq[String] = Seq(ActiveReportTemplate.DATE) ++ dimensions

    val metricCols = Seq(
      ActiveReportTemplate.A1,
      ActiveReportTemplate.TOTAL_A1_ACTIONS,
      ActiveReportTemplate.A7,
      ActiveReportTemplate.TOTAL_A7_ACTIONS,
      ActiveReportTemplate.A30,
      ActiveReportTemplate.TOTAL_A30_ACTIONS,
      ActiveReportTemplate.AN,
      ActiveReportTemplate.TOTAL_AN_ACTIONS,
      ActiveReportTemplate.A0,
      ActiveReportTemplate.TOTAL_A0_ACTIONS
    )

    a1ResultDf
      .join(a7ResultDf, joinCols, "outer")
      .join(a30ResultDf, joinCols, "outer")
      .join(anResultDf, joinCols, "outer")
      .join(a0ResultDf, joinCols, "outer")
      .na
      .fill(0L, metricCols)
      .groupBy(joinCols.map(col): _*)
      .agg(
        sum(col(ActiveReportTemplate.A1)).as(ActiveReportTemplate.A1),
        sum(col(ActiveReportTemplate.TOTAL_A1_ACTIONS)).as(ActiveReportTemplate.TOTAL_A1_ACTIONS),
        sum(col(ActiveReportTemplate.A7)).as(ActiveReportTemplate.A7),
        sum(col(ActiveReportTemplate.TOTAL_A7_ACTIONS)).as(ActiveReportTemplate.TOTAL_A7_ACTIONS),
        sum(col(ActiveReportTemplate.A30)).as(ActiveReportTemplate.A30),
        sum(col(ActiveReportTemplate.TOTAL_A30_ACTIONS)).as(ActiveReportTemplate.TOTAL_A30_ACTIONS),
        sum(col(ActiveReportTemplate.AN)).as(ActiveReportTemplate.AN),
        sum(col(ActiveReportTemplate.TOTAL_AN_ACTIONS)).as(ActiveReportTemplate.TOTAL_AN_ACTIONS),
        sum(col(ActiveReportTemplate.A0)).as(ActiveReportTemplate.A0),
        sum(col(ActiveReportTemplate.TOTAL_A0_ACTIONS)).as(ActiveReportTemplate.TOTAL_A0_ACTIONS)
      )

  }

  private def buildA7Df(
      jobId: String,
      date: Long,
      a1Df: DataFrame,
      config: Config,
      namePrefix: Option[String] = None,
      schema: Option[StructType] = None
  ): DataFrame = {
    val startTime = date - 1.days.toMillis - (7 - 1 - 1).days.toMillis
    val endTime = date - 1.days.toMillis

    loadAllA1DfFromS3(config, jobId, startTime, endTime, namePrefix, schema) match {
      case Some(a7Df) =>
        a7Df
          .unionByName(a1Df, allowMissingColumns = true)
          .withColumn(ActiveReportTemplate.DATE, lit(date))
      case None => a1Df
    }

  }

  private def buildA30Df(
      jobId: String,
      date: Long,
      a7Df: DataFrame,
      config: Config,
      namePrefix: Option[String] = None,
      schema: Option[StructType] = None
  ): DataFrame = {
    val startTime = date - 7.days.toMillis - (30 - 1 - 7).days.toMillis
    val endTime = date - 7.days.toMillis

    loadAllA1DfFromS3(config, jobId, startTime, endTime, namePrefix, schema) match {
      case Some(df) =>
        df.unionByName(a7Df, allowMissingColumns = true)
          .withColumn(ActiveReportTemplate.DATE, lit(date))
      case None => a7Df
    }
  }

  private def executeResultDf(
      date: Long,
      df: DataFrame,
      dimensions: Seq[String],
      uniqueFields: Seq[String],
      generator: Row => Seq[Row],
      uniqueMeasure: String,
      totalActionMeasure: String
  ): DataFrame = {
    df.withColumn(ActiveReportTemplate.DATE, lit(date))
      .flatMap(generator)(RowEncoder(df.schema))
      .groupBy((Seq(ActiveReportTemplate.DATE) ++ dimensions).map(col): _*)
      .agg(
        countDistinct(col(uniqueFields.head), uniqueFields.tail.map(col): _*).as(uniqueMeasure),
        sum(col(ActiveReportTemplate.TOTAL_ACTIONS)).as(totalActionMeasure)
      )
  }
}
