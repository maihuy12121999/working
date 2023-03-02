package rever.etl.agentcrm.events
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{callUDF, col, count, when}
import org.apache.spark.sql.types.StructType
import org.apache.spark.sql.{DataFrame, Row}
import rever.etl.agentcrm.config.FieldConfig
import rever.etl.agentcrm.events.reader.EventReader
import rever.etl.agentcrm.events.writer.EventAnalysisWriter
import rever.etl.agentcrm.extension.CommonMixin
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.{Output, Table}
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.api.udf.RUdfUtils
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

class EventAnalysis extends FlowMixin with CommonMixin {
  @Output(writer = classOf[EventAnalysisWriter])
  @Table("TopEventActionDF")
  def build(
      @Table(
        name = "segment_tracking.raw_data_normalized",
        reader = classOf[EventReader]
      ) webRawEventDf: DataFrame,
      config: Config
  ): DataFrame = {
    val df = enhanceClientOSAndPlatform(webRawEventDf, FieldConfig.USER_AGENT)

    val standardizedDf = standardizeStringCols(
      df,
      Seq(
        FieldConfig.CLIENT_PLATFORM,
        FieldConfig.CLIENT_OS,
        FieldConfig.PAGE,
        FieldConfig.EVENT
      ),
      "unknown"
    )

    enhanceDepartments(config, standardizedDf)
      .groupBy(
        col(FieldConfig.DATE),
        col(FieldConfig.DEPARTMENT),
        col(FieldConfig.CLIENT_PLATFORM),
        col(FieldConfig.CLIENT_OS),
        col(FieldConfig.PAGE),
        col(FieldConfig.EVENT)
      )
      .agg(count(FieldConfig.EVENT).as(FieldConfig.TOTAL_ACTIONS))
  }

  private def enhanceDepartments(config: Config, df: DataFrame): DataFrame = {

    val eventActionSchema = StructType(
      Array(
        StructField(FieldConfig.DATE, LongType, nullable = true),
        StructField(FieldConfig.DEPARTMENT, StringType, nullable = true),
        StructField(FieldConfig.CLIENT_PLATFORM, StringType, nullable = true),
        StructField(FieldConfig.CLIENT_OS, StringType, nullable = true),
        StructField(FieldConfig.PAGE, StringType, nullable = true),
        StructField(FieldConfig.EVENT, StringType, nullable = true)
      )
    )

    df.mapPartitions { rows =>
      val rowSeq = rows.toSeq
      val userIds = rowSeq.map(_.getAs[String](FieldConfig.USER_ID)).filterNot(_ == null).filterNot(_.isEmpty)
      val userTeamMap = mGetUserTeams(userIds, config)
      rowSeq.map { row =>
        val date = row.getAs[Long](FieldConfig.DATE)
        val clientPlatform = row.getAs[String](FieldConfig.CLIENT_PLATFORM)
        val clientOs = row.getAs[String](FieldConfig.CLIENT_OS)
        val page = row.getAs[String](FieldConfig.PAGE)
        val event = row.getAs[String](FieldConfig.EVENT)
        val userId = row.getAs[String](FieldConfig.USER_ID)
        val department = toDepartment(userId, userTeamMap, config)
        Row(date, department, clientPlatform, clientOs, page, event)

      }.toIterator

    }(RowEncoder(eventActionSchema))
  }

}
