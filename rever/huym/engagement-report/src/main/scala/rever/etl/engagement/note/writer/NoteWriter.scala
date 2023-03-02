package rever.etl.engagement.note.writer

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{Dataset, Row}
import rever.etl.engagement.domain.NoteFields
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.extensions.RapIngestWriterMixin
import rever.etl.rsparkflow.utils.JsonUtils

class NoteWriter extends SinkWriter with RapIngestWriterMixin {
  override def write(tableName: String, df: Dataset[Row], config: Config): Dataset[Row] = {
    val topic = config.get("new_note_engagement_topic")
    ingestDataframe(config, df, topic)(toNoteRecord)
    mergeIfRequired(config,df,topic)
    df
  }
  private def toNoteRecord(row: Row): JsonNode = {
    val recordMap = Map(
      NoteFields.DATE -> row.getAs[Long](NoteFields.DATE),
      NoteFields.INTERVAL -> row.getAs[String](NoteFields.INTERVAL),
      NoteFields.BUSINESS_UNIT -> row.getAs[String](NoteFields.BUSINESS_UNIT),
      NoteFields.MARKET_CENTER_ID -> row.getAs[String](NoteFields.MARKET_CENTER_ID),
      NoteFields.TEAM_ID -> row.getAs[String](NoteFields.TEAM_ID),
      NoteFields.USER_TYPE -> row.getAs[String](NoteFields.USER_TYPE),
      NoteFields.AGENT_ID -> row.getAs[String](NoteFields.AGENT_ID),
      NoteFields.SOURCE -> row.getAs[String](NoteFields.SOURCE),
      NoteFields.CID -> row.getAs[String](NoteFields.CID),
      NoteFields.STATUS -> row.getAs[String](NoteFields.STATUS),
      NoteFields.TOTAL_NOTE -> row.getAs[Long](NoteFields.TOTAL_NOTE)
    )
    JsonUtils.toJsonNode(JsonUtils.toJson(recordMap))
  }
}
