package rever.etl.agentcrm.page_view

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{callUDF, col, count, when}
import org.apache.spark.sql.types.{DataTypes, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import rever.etl.agentcrm.config.FieldConfig
import rever.etl.agentcrm.extension.CommonMixin
import rever.etl.agentcrm.page_view.reader.PageViewReader
import rever.etl.agentcrm.page_view.writer.PageViewWriter
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.{Output, Table}
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.api.udf.RUdfUtils
class PageViewAnalysis extends FlowMixin with CommonMixin {

  @Output(writer = classOf[PageViewWriter])
  @Table("TopPageViewDF")
  def build(
      @Table(
        name = "web_pageview_raw_events",
        reader = classOf[PageViewReader]
      ) pageViewEventDf: DataFrame,
      config: Config
  ): DataFrame = {
    val df = enhanceClientOSAndPlatform(pageViewEventDf, FieldConfig.USER_AGENT)

    val standardizedDf = standardizeStringCols(
      df,
      Seq(
        FieldConfig.CLIENT_PLATFORM,
        FieldConfig.CLIENT_OS,
        FieldConfig.PAGE
      ),
      "unknown"
    )

    enhanceDepartments(config, standardizedDf)
      .groupBy(
        col(FieldConfig.DEPARTMENT),
        col(FieldConfig.DATE),
        col(FieldConfig.CLIENT_PLATFORM),
        col(FieldConfig.CLIENT_OS),
        col(FieldConfig.PAGE)
      )
      .agg(count(FieldConfig.PAGE).as(FieldConfig.TOTAL_VIEWS))
  }

  private def enhanceDepartments(config: Config, df: DataFrame): DataFrame = {
    val pageViewSchema = StructType(
      Array(
        StructField(FieldConfig.DATE, DataTypes.LongType, nullable = true),
        StructField(FieldConfig.DEPARTMENT, DataTypes.StringType, nullable = true),
        StructField(FieldConfig.CLIENT_PLATFORM, DataTypes.StringType, nullable = true),
        StructField(FieldConfig.CLIENT_OS, DataTypes.StringType, nullable = true),
        StructField(FieldConfig.PAGE, DataTypes.StringType, nullable = true)
      )
    )

    df.mapPartitions { rows =>
      val rowSeq = rows.toSeq
      val userIds = rowSeq
        .map(_.getAs[String](FieldConfig.USER_ID))
        .filterNot(_ == null)
        .filterNot(_.isEmpty)
        .distinct

      val userTeamMap = mGetUserTeams(userIds, config)

      rowSeq.map { row =>
        val date = row.getAs[Long](FieldConfig.DATE)
        val clientPlatform = row.getAs[String](FieldConfig.CLIENT_PLATFORM)
        val clientOs = row.getAs[String](FieldConfig.CLIENT_OS)
        val page = row.getAs[String](FieldConfig.PAGE)

        val userId = row.getAs[String](FieldConfig.USER_ID)
        val department = toDepartment(userId, userTeamMap, config)

        Row(date, department, clientPlatform, clientOs, page)

      }.toIterator

    }(RowEncoder(pageViewSchema))
  }

}
