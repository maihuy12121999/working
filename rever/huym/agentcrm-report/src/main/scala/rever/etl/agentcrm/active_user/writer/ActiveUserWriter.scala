package rever.etl.agentcrm.active_user.writer

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{DataFrame, Row}
import rever.etl.agentcrm.active_user.ActiveUserHelper
import rever.etl.agentcrm.config.FieldConfig
import rever.etl.rsparkflow.RapIngestWriterMixin
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.utils.JsonUtils

class ActiveUserWriter extends SinkWriter with RapIngestWriterMixin {
  override def write(tableName: String, dataFrame: DataFrame, config: Config): DataFrame = {
    val topic = config.get("active_user_topic")

    ingestDataframe(config, dataFrame, topic)(toActiveUserRecord, 1000)

    mergeIfRequired(config, dataFrame, topic)

    dataFrame
  }

  private def toActiveUserRecord(row: Row): JsonNode = {
    val jsonStr = JsonUtils.toJson(
      Map(
        FieldConfig.DATE -> row.getAs[Long](FieldConfig.DATE),
        FieldConfig.DEPARTMENT -> row.getAs[String](FieldConfig.DEPARTMENT),
        FieldConfig.CLIENT_PLATFORM -> row.getAs[String](FieldConfig.CLIENT_PLATFORM),
        FieldConfig.CLIENT_OS -> row.getAs[String](FieldConfig.CLIENT_OS),
        FieldConfig.PAGE -> row.getAs[String](FieldConfig.PAGE),
        FieldConfig.EVENT -> row.getAs[String](FieldConfig.EVENT),
        ActiveUserHelper.A1 -> row.getAs[Long](ActiveUserHelper.A1),
        ActiveUserHelper.TOTAL_A1_ACTIONS -> row.getAs[Long](ActiveUserHelper.TOTAL_A1_ACTIONS),
        ActiveUserHelper.A7 -> row.getAs[Long](ActiveUserHelper.A7),
        ActiveUserHelper.TOTAL_A7_ACTIONS -> row.getAs[Long](ActiveUserHelper.TOTAL_A7_ACTIONS),
        ActiveUserHelper.A30 -> row.getAs[Long](ActiveUserHelper.A30),
        ActiveUserHelper.TOTAL_A30_ACTIONS -> row.getAs[Long](ActiveUserHelper.TOTAL_A30_ACTIONS),
        ActiveUserHelper.AN -> row.getAs[Long](ActiveUserHelper.AN),
        ActiveUserHelper.TOTAL_AN_ACTIONS -> row.getAs[Long](ActiveUserHelper.TOTAL_AN_ACTIONS),
        ActiveUserHelper.A0 -> row.getAs[Long](ActiveUserHelper.A0),
        ActiveUserHelper.TOTAL_A0_ACTIONS -> row.getAs[Long](ActiveUserHelper.TOTAL_A0_ACTIONS)
      )
    )
    JsonUtils.toJsonNode(jsonStr)
  }
}
