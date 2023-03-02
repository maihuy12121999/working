package rever.etl.call.jobs.non_call_users.writer

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{Dataset, Row}
import rever.etl.call.domain.NonCallAgentFields
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.extensions.RapIngestWriterMixin
import rever.etl.rsparkflow.utils.JsonUtils

class NonCallAgentWriter extends SinkWriter with RapIngestWriterMixin{

  override def write(tableName: String, df: Dataset[Row], config: Config): Dataset[Row] = {
    val topic = config.get("non_call_user_topic")
    ingestDataframe(config,df,topic)(toCallNonUsingAgentRecord)
    mergeIfRequired(config,df, topic)
    df
  }
  private def toCallNonUsingAgentRecord(row:Row) :JsonNode = {
    val recordMap = Map(
      NonCallAgentFields.DATA_TYPE->row.getAs[String](NonCallAgentFields.DATA_TYPE),
      NonCallAgentFields.DATE->row.getAs[Long](NonCallAgentFields.DATE),
      NonCallAgentFields.USERNAME->row.getAs[String](NonCallAgentFields.USERNAME),
      NonCallAgentFields.TEAM_ID->row.getAs[String](NonCallAgentFields.TEAM_ID),
      NonCallAgentFields.MARKET_CENTER_ID->row.getAs[String](NonCallAgentFields.MARKET_CENTER_ID),
      NonCallAgentFields.CALL_SERVICE->row.getAs[String](NonCallAgentFields.CALL_SERVICE)
    )
    JsonUtils.toJsonNode(JsonUtils.toJson(recordMap))
  }
}