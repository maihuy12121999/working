package rever.etl.support.cs2_call_cta_analysis.writer

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{DataFrame, Row}
import rever.rsparkflow.spark.api.SinkWriter
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.extensions.RapIngestWriterMixin
import rever.rsparkflow.spark.utils.JsonUtils

class CallCtaEventWriter extends SinkWriter with RapIngestWriterMixin {

  override def write(tableName: String, dataFrame: DataFrame, config: Config): DataFrame = {

    val topic = config.get("cs2_call_cta_event_topic")

    ingestDataframe(config, dataFrame, topic)(toReportRecord)

    mergeIfRequired(config, dataFrame, topic)
    dataFrame
  }

  private def toReportRecord(row: Row): JsonNode = {
    val jsonRow = JsonUtils.toJson(
      Map(
        "timestamp" -> row.getAs[Long]("timestamp"),
        "message_id" -> row.getAs[String]("message_id"),
        "anonymous_id" -> row.getAs[String]("anonymous_id"),
        "user_id" -> row.getAs[String]("user_id"),
        "user_phone_number" -> row.getAs[String]("user_phone_number"),
        "url" -> row.getAs[String]("url"),
        "source_type" -> row.getAs[String]("source_type"),
        "source_code" -> row.getAs[String]("source_code"),
        "owner" -> row.getAs[String]("owner"),
        "event" -> row.getAs[String]("event"),
        "os" -> row.getAs[String]("os"),
        "platform" -> row.getAs[String]("platform")
      )
    )
    JsonUtils.toJsonNode(jsonRow)
  }

}
