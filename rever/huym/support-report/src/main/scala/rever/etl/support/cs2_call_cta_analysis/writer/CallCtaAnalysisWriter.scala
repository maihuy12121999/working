package rever.etl.support.cs2_call_cta_analysis.writer

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{DataFrame, Row}
import rever.rsparkflow.spark.api.SinkWriter
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.extensions.RapIngestWriterMixin
import rever.rsparkflow.spark.utils.JsonUtils

class CallCtaAnalysisWriter extends SinkWriter with RapIngestWriterMixin {

  override def write(tableName: String, dataFrame: DataFrame, config: Config): DataFrame = {
    val topic = config.get("cs2_call_cta_analysis_topic")

    ingestDataframe(config, dataFrame, topic)(toReportRecord)

    mergeIfRequired(config, dataFrame, topic)
    dataFrame
  }

  private def toReportRecord(row: Row): JsonNode = {
    val jsonRow = JsonUtils.toJson(
      Map(
        "clicked_at" -> row.getAs[Long]("clicked_at"),
        "message_id" -> row.getAs[String]("message_id"),
        "anonymous_id" -> row.getAs[String]("anonymous_id"),
        "user_id" -> row.getAs[String]("user_id"),
        "user_phone_number" -> row.getAs[String]("user_phone_number"),
        "url" -> row.getAs[String]("url"),
        "source_type" -> row.getAs[String]("source_type"),
        "source_code" -> row.getAs[String]("source_code"),
        "event" -> row.getAs[String]("event"),
        "os" -> row.getAs[String]("os"),
        "platform" -> row.getAs[String]("platform"),
        "call_id" -> row.getAs[String]("call_id"),
        "caller_phone" -> row.getAs[String]("caller_phone"),
        "receiver_phone" -> row.getAs[String]("receiver_phone"),
        "extension" -> row.getAs[String]("extension"),
        "call_status" -> row.getAs[String]("call_status"),
        "duration" -> row.getAs[Int]("duration"),
        "call_start_at" -> row.getAs[Long]("call_start_at"),
        "call_end_at" -> row.getAs[Long]("call_end_at"),
        "click_and_call_duration_gap" -> row.getAs[Long]("click_and_call_duration_gap")
      )
    )
    JsonUtils.toJsonNode(jsonRow)
  }

}
