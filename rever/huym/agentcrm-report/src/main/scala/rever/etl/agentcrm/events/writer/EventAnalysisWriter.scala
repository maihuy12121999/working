package rever.etl.agentcrm.events.writer

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{Dataset, Row}
import rever.etl.agentcrm.config.FieldConfig
import rever.etl.rsparkflow.RapIngestWriterMixin
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.utils.JsonUtils

class EventAnalysisWriter extends SinkWriter with RapIngestWriterMixin {
  override def write(s: String, df: Dataset[Row], config: Config): Dataset[Row] = {
    val topic = config.get("event_topic")
    ingestDataframe(config, df, topic)(toEventRecord)
    df
  }

  private def toEventRecord(row: Row): JsonNode = {
    val recordMap = Map(
      FieldConfig.DATE -> row.getAs[Long](FieldConfig.DATE),
      FieldConfig.DEPARTMENT -> row.getAs[String](FieldConfig.DEPARTMENT),
      FieldConfig.CLIENT_PLATFORM -> row.getAs[String](FieldConfig.CLIENT_PLATFORM),
      FieldConfig.CLIENT_OS -> row.getAs[String](FieldConfig.CLIENT_OS),
      FieldConfig.PAGE -> row.getAs[String](FieldConfig.PAGE),
      FieldConfig.EVENT -> row.getAs[String](FieldConfig.EVENT),
      FieldConfig.TOTAL_ACTIONS -> row.getAs[Long](FieldConfig.TOTAL_ACTIONS)
    )
    JsonUtils.toJsonNode(JsonUtils.toJson(recordMap))
  }
}
