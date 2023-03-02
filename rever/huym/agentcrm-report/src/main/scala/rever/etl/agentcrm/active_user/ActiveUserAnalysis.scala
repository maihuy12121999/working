package rever.etl.agentcrm.active_user

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.storage.StorageLevel
import rever.etl.agentcrm.active_user.reader.ActiveUserReader
import rever.etl.agentcrm.active_user.writer.ActiveUserWriter
import rever.etl.agentcrm.config.FieldConfig
import rever.etl.agentcrm.extension.CommonMixin
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.{Output, Table}
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.api.udf.RUdfUtils

import scala.concurrent.duration.DurationInt

class ActiveUserAnalysis extends FlowMixin with CommonMixin {
  @Output(writer = classOf[ActiveUserWriter])
  @Table("active_user")
  def build(
      @Table(
        name = "web_raw_events",
        reader = classOf[ActiveUserReader]
      ) webRawEventDf: DataFrame,
      config: Config
  ): DataFrame = {

    val reportTime = config.getDailyReportTime
    val activeUserDf = enhanceAndCleanupDf(webRawEventDf, config)
      .groupBy(
        col(FieldConfig.DATE),
        col(FieldConfig.DEPARTMENT),
        col(FieldConfig.CLIENT_PLATFORM),
        col(FieldConfig.CLIENT_OS),
        col(FieldConfig.PAGE),
        col(FieldConfig.EVENT),
        col(FieldConfig.USER_ID)
      )
      .agg(count(col(FieldConfig.PAGE)).as(ActiveUserHelper.TOTAL_ACTIONS))
      .persist(StorageLevel.MEMORY_AND_DISK_2)

    exportA1DfToS3(config.getJobId, reportTime, activeUserDf, config)

    val previousA0Df: Option[DataFrame] = getPreviousA0DfFromS3(config.getJobId, reportTime, config)

    val a0Df = previousA0Df match {
      case Some(previousA0Df) =>
        previousA0Df
          .unionByName(activeUserDf, allowMissingColumns = true)
          .withColumn(FieldConfig.DATE, lit(reportTime))
          .groupBy(
            col(FieldConfig.DATE),
            col(FieldConfig.DEPARTMENT),
            col(FieldConfig.CLIENT_PLATFORM),
            col(FieldConfig.CLIENT_OS),
            col(FieldConfig.PAGE),
            col(FieldConfig.EVENT),
            col(FieldConfig.USER_ID)
          )
          .agg(sum(col(ActiveUserHelper.TOTAL_ACTIONS)).as(ActiveUserHelper.TOTAL_ACTIONS))
          .persist(StorageLevel.MEMORY_AND_DISK_2)
      case _ => activeUserDf
    }

    val anDf = previousA0Df match {
      case Some(previousA0Df) =>
        activeUserDf.join(
          previousA0Df,
          activeUserDf(FieldConfig.USER_ID) === previousA0Df(FieldConfig.USER_ID),
          "leftanti"
        )
      case _ => activeUserDf
    }

    exportAnDfToS3(config.getJobId, reportTime, anDf, config)
    exportA0DfToS3(config.getJobId, reportTime, a0Df, config)

    val a1ResultDf = buildA1Df(activeUserDf)
    val a7ResultDf = buildA7Df(activeUserDf, config)
    val a30ResultDf = buildA30Df(activeUserDf, config)
    val anResultDf = buildAnDf(anDf)
    val a0ResultDf = buildA0Df(a0Df)

    val joinCols: Seq[String] = Seq(
      FieldConfig.DATE,
      FieldConfig.DEPARTMENT,
      FieldConfig.CLIENT_PLATFORM,
      FieldConfig.CLIENT_OS,
      FieldConfig.PAGE,
      FieldConfig.EVENT
    )

    val metricCols = Seq(
      ActiveUserHelper.A1,
      ActiveUserHelper.TOTAL_A1_ACTIONS,
      ActiveUserHelper.A7,
      ActiveUserHelper.TOTAL_A7_ACTIONS,
      ActiveUserHelper.A30,
      ActiveUserHelper.TOTAL_A30_ACTIONS,
      ActiveUserHelper.AN,
      ActiveUserHelper.TOTAL_AN_ACTIONS,
      ActiveUserHelper.A0,
      ActiveUserHelper.TOTAL_A0_ACTIONS
    )

    a1ResultDf
      .join(a7ResultDf, joinCols, "outer")
      .join(a30ResultDf, joinCols, "outer")
      .join(anResultDf, joinCols, "outer")
      .join(a0ResultDf, joinCols, "outer")
      .na
      .fill(0L, metricCols)
      .groupBy(
        col(FieldConfig.DATE),
        col(FieldConfig.DEPARTMENT),
        col(FieldConfig.CLIENT_PLATFORM),
        col(FieldConfig.CLIENT_OS),
        col(FieldConfig.PAGE),
        col(FieldConfig.EVENT)
      )
      .agg(
        sum(col(ActiveUserHelper.A1)).as(ActiveUserHelper.A1),
        sum(col(ActiveUserHelper.TOTAL_A1_ACTIONS)).as(ActiveUserHelper.TOTAL_A1_ACTIONS),
        sum(col(ActiveUserHelper.A7)).as(ActiveUserHelper.A7),
        sum(col(ActiveUserHelper.TOTAL_A7_ACTIONS)).as(ActiveUserHelper.TOTAL_A7_ACTIONS),
        sum(col(ActiveUserHelper.A30)).as(ActiveUserHelper.A30),
        sum(col(ActiveUserHelper.TOTAL_A30_ACTIONS)).as(ActiveUserHelper.TOTAL_A30_ACTIONS),
        sum(col(ActiveUserHelper.AN)).as(ActiveUserHelper.AN),
        sum(col(ActiveUserHelper.TOTAL_AN_ACTIONS)).as(ActiveUserHelper.TOTAL_AN_ACTIONS),
        sum(col(ActiveUserHelper.A0)).as(ActiveUserHelper.A0),
        sum(col(ActiveUserHelper.TOTAL_A0_ACTIONS)).as(ActiveUserHelper.TOTAL_A0_ACTIONS)
      )

  }

  private def enhanceAndCleanupDf(activeUserRawDf: DataFrame, config: Config): DataFrame = {
    val df = enhanceClientOSAndPlatform(activeUserRawDf, FieldConfig.USER_AGENT)
      .withColumn(
        FieldConfig.DATE,
        expr(s"""BIGINT(to_timestamp(to_date(timestamp_millis(${FieldConfig.TIMESTAMP}),"dd/MM/yyyy")))*1000""")
      )
      .drop(col(FieldConfig.USER_AGENT))
      .mapPartitions(rows => rows.toSeq.grouped(500).flatMap(enhanceDepartments(_, config)))(
        RowEncoder(ActiveUserHelper.schema)
      )

    standardizeStringCols(
      df,
      Seq(
        FieldConfig.DEPARTMENT,
        FieldConfig.CLIENT_PLATFORM,
        FieldConfig.CLIENT_OS,
        FieldConfig.PAGE,
        FieldConfig.EVENT
      ),
      "unknown"
    )
  }

  private def enhanceDepartments(rows: Seq[Row], config: Config): Seq[Row] = {

    val userIds = rows
      .map(_.getAs[String](FieldConfig.USER_ID))
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .distinct

    val userTeamMap = mGetUserTeams(userIds, config)

    rows.map { row =>
      val date = row.getAs[Long](FieldConfig.DATE)
      val clientPlatForm = row.getAs[String](FieldConfig.CLIENT_PLATFORM)
      val clientOs = row.getAs[String](FieldConfig.CLIENT_OS)
      val page = row.getAs[String](FieldConfig.PAGE)
      val event = row.getAs[String](FieldConfig.EVENT)

      val userId = row.getAs[String](FieldConfig.USER_ID)
      val department = toDepartment(userId, userTeamMap, config)

      Row(date, department, clientPlatForm, clientOs, page, event, userId)

    }
  }

  private def buildA1Df(dailyUserDf: DataFrame): DataFrame = {
    dailyUserDf
      .flatMap(ActiveUserHelper.generateRow)(RowEncoder(dailyUserDf.schema))
      .groupBy(
        col(FieldConfig.DATE),
        col(FieldConfig.DEPARTMENT),
        col(FieldConfig.CLIENT_PLATFORM),
        col(FieldConfig.CLIENT_OS),
        col(FieldConfig.PAGE),
        col(FieldConfig.EVENT)
      )
      .agg(
        countDistinct(col(FieldConfig.USER_ID)).as(ActiveUserHelper.A1),
        sum(col(ActiveUserHelper.TOTAL_ACTIONS)).as(ActiveUserHelper.TOTAL_A1_ACTIONS)
      )
  }

  private def buildA7Df(activeUserDf: DataFrame, config: Config): DataFrame = {
    val startOfTime = config.getDailyReportTime - 6.days.toMillis
    val endOfTime = config.getDailyReportTime

    val a7Df = loadAllA1DfFromS3(config, config.getJobId, startOfTime, endOfTime) match {
      case Some(a7Df) =>
        a7Df
          .withColumn(FieldConfig.DATE, lit(endOfTime))
          .flatMap(ActiveUserHelper.generateRow)(RowEncoder(activeUserDf.schema))
          .groupBy(
            col(FieldConfig.DATE),
            col(FieldConfig.DEPARTMENT),
            col(FieldConfig.CLIENT_PLATFORM),
            col(FieldConfig.CLIENT_OS),
            col(FieldConfig.PAGE),
            col(FieldConfig.EVENT)
          )
          .agg(
            countDistinct(col(FieldConfig.USER_ID)).as(ActiveUserHelper.A7),
            sum(col(ActiveUserHelper.TOTAL_ACTIONS)).as(ActiveUserHelper.TOTAL_A7_ACTIONS)
          )
      case None => SparkSession.active.emptyDataset(RowEncoder(ActiveUserHelper.a7Schema))
    }
    a7Df
  }

  private def buildA30Df(activeUserDf: DataFrame, config: Config): DataFrame = {
    val startOfTime = config.getDailyReportTime - 29.days.toMillis
    val endOfTime = config.getDailyReportTime

    val a30Df = loadAllA1DfFromS3(config, config.getJobId, startOfTime, endOfTime) match {
      case Some(a30Df) =>
        a30Df
          .withColumn(FieldConfig.DATE, lit(endOfTime))
          .flatMap(ActiveUserHelper.generateRow)(RowEncoder(activeUserDf.schema))
          .groupBy(
            col(FieldConfig.DATE),
            col(FieldConfig.DEPARTMENT),
            col(FieldConfig.CLIENT_PLATFORM),
            col(FieldConfig.CLIENT_OS),
            col(FieldConfig.PAGE),
            col(FieldConfig.EVENT)
          )
          .agg(
            countDistinct(col(FieldConfig.USER_ID)).as(ActiveUserHelper.A30),
            sum(col(ActiveUserHelper.TOTAL_ACTIONS)).as(ActiveUserHelper.TOTAL_A30_ACTIONS)
          )
      case None => SparkSession.active.emptyDataset(RowEncoder(ActiveUserHelper.a30Schema))
    }
    a30Df
  }

  private def buildAnDf(anDf: DataFrame): DataFrame = {
    anDf
      .flatMap(ActiveUserHelper.generateRow)(RowEncoder(anDf.schema))
      .groupBy(
        col(FieldConfig.DATE),
        col(FieldConfig.DEPARTMENT),
        col(FieldConfig.CLIENT_PLATFORM),
        col(FieldConfig.CLIENT_OS),
        col(FieldConfig.PAGE),
        col(FieldConfig.EVENT)
      )
      .agg(
        countDistinct(col(FieldConfig.USER_ID)).as(ActiveUserHelper.AN),
        sum(col(ActiveUserHelper.TOTAL_ACTIONS)).as(ActiveUserHelper.TOTAL_AN_ACTIONS)
      )
  }

  private def buildA0Df(a0Df: DataFrame): DataFrame = {
    a0Df
      .flatMap(ActiveUserHelper.generateRow)(RowEncoder(a0Df.schema))
      .groupBy(
        col(FieldConfig.DATE),
        col(FieldConfig.DEPARTMENT),
        col(FieldConfig.CLIENT_PLATFORM),
        col(FieldConfig.CLIENT_OS),
        col(FieldConfig.PAGE),
        col(FieldConfig.EVENT)
      )
      .agg(
        countDistinct(col(FieldConfig.USER_ID)).as(ActiveUserHelper.A0),
        sum(col(ActiveUserHelper.TOTAL_ACTIONS)).as(ActiveUserHelper.TOTAL_A0_ACTIONS)
      )
  }
}
