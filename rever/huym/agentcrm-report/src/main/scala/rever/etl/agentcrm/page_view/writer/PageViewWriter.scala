package rever.etl.agentcrm.page_view.writer

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{Dataset, Row}
import rever.etl.agentcrm.config.FieldConfig
import rever.etl.rsparkflow.RapIngestWriterMixin
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.utils.JsonUtils

class PageViewWriter extends SinkWriter with RapIngestWriterMixin {
  override def write(s: String, df: Dataset[Row], config: Config): Dataset[Row] = {
    val topic = config.get("page_view_topic")
    ingestDataframe(config, df, topic)(toPageViewRecord)
    df
  }
  private def toPageViewRecord(row: Row): JsonNode = {
    val recordMap = Map(
      FieldConfig.DATE -> row.getAs[Long](FieldConfig.DATE),
      FieldConfig.DEPARTMENT -> row.getAs[String](FieldConfig.DEPARTMENT),
      FieldConfig.CLIENT_PLATFORM -> row.getAs[String](FieldConfig.CLIENT_PLATFORM),
      FieldConfig.CLIENT_OS -> row.getAs[String](FieldConfig.CLIENT_OS),
      FieldConfig.PAGE -> row.getAs[String](FieldConfig.PAGE),
      FieldConfig.TOTAL_VIEWS -> row.getAs[Long](FieldConfig.TOTAL_VIEWS)
    )
    JsonUtils.toJsonNode(JsonUtils.toJson(recordMap))
  }
}
