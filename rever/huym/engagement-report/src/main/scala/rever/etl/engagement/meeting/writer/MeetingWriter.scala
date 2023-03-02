package rever.etl.engagement.meeting.writer

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{Dataset, Row}
import rever.etl.engagement.domain.MeetingFields
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.extensions.RapIngestWriterMixin
import rever.etl.rsparkflow.utils.JsonUtils

class MeetingWriter extends SinkWriter with RapIngestWriterMixin {
  override def write(s: String, df: Dataset[Row], config: Config): Dataset[Row] = {
    val topic = config.get("new_meeting_engagement_topic")
    ingestDataframe(config, df, topic)(toMeetingRecord)
    mergeIfRequired(config,df,topic)
    df
  }
  private def toMeetingRecord(row: Row): JsonNode = {
    val recordMap = Map(
      MeetingFields.DATE -> row.getAs[Long](MeetingFields.DATE),
      MeetingFields.INTERVAL -> row.getAs[String](MeetingFields.INTERVAL),
      MeetingFields.BUSINESS_UNIT -> row.getAs[String](MeetingFields.BUSINESS_UNIT),
      MeetingFields.MARKET_CENTER_ID -> row.getAs[String](MeetingFields.MARKET_CENTER_ID),
      MeetingFields.TEAM_ID -> row.getAs[String](MeetingFields.TEAM_ID),
      MeetingFields.USER_TYPE -> row.getAs[String](MeetingFields.USER_TYPE),
      MeetingFields.AGENT_ID -> row.getAs[String](MeetingFields.AGENT_ID),
      MeetingFields.SOURCE -> row.getAs[String](MeetingFields.SOURCE),
      MeetingFields.CID -> row.getAs[String](MeetingFields.CID),
      MeetingFields.STATUS -> row.getAs[String](MeetingFields.STATUS),
      MeetingFields.TOTAL_MEETING -> row.getAs[Long](MeetingFields.TOTAL_MEETING),
      MeetingFields.TOTAL_DURATION -> row.getAs[Long](MeetingFields.TOTAL_DURATION),
      MeetingFields.MIN_DURATION -> row.getAs[Long](MeetingFields.MIN_DURATION),
      MeetingFields.MAX_DURATION -> row.getAs[Long](MeetingFields.MAX_DURATION)
    )
    JsonUtils.toJsonNode(JsonUtils.toJson(recordMap))
  }
}
