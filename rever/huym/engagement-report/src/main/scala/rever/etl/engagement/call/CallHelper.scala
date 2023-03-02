package rever.etl.engagement.call

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.Row
import rever.etl.rsparkflow.utils.{JsonUtils, Utils}

object CallHelper {

  final val DATA_TYPE = "data_type"

  final val TIMESTAMP = "timestamp"
  final val DATE = "date"

  final val BUSINESS_UNIT = "business_unit"
  final val MARKET_CENTER_ID = "market_center_id"
  final val TEAM_ID = "team_id"
  final val USER_TYPE = "user_type"
  final val AGENT_ID = "agent_id"
  final val CREATOR_ID = "creator_id"
  final val SOURCE = "source"
  final val CONTACTS = "contacts"
  final val CIDS = "cids"
  final val CID = "cid"
  final val DIRECTION = "direction"
  final val CALL_STATUS = "call_status"
  final val DURATION = "duration"
  final val CALL_ID = "call_id"
  final val TOTAL_CALL = "total_call"
  final val TOTAL_DURATION = "total_duration"
  final val MIN_DURATION = "min_duration"
  final val MAX_DURATION = "max_duration"

  final val TYPE = "type"

  def generateRow(row: Row): Seq[Row] = {
    val cartesianRows = Utils.cartesianProduct(
      List(
        List(row.getAs[Long](DATE)),
        List(row.getAs[String](BUSINESS_UNIT), "all"),
        List(row.getAs[String](MARKET_CENTER_ID), "all"),
        List(row.getAs[String](TEAM_ID), "all"),
        List(row.getAs[String](USER_TYPE)),
        List(row.getAs[String](AGENT_ID)),
        List(row.getAs[String](CREATOR_ID)),
        List(row.getAs[String](SOURCE)),
        List(row.getAs[String](CID), "all"),
        List(row.getAs[String](DIRECTION)),
        List(row.getAs[String](CALL_STATUS)),
        List(row.getAs[Long](DURATION)),
        List(row.getAs[String](CALL_ID))
      )
    )
    cartesianRows.flatMap(r => Seq(Row.fromSeq(r)))
  }

  def toNewCallRecord(row: Row): JsonNode = {
    val jsonRow = JsonUtils.toJson(
      Map(
        "data_type" -> row.getAs[String](DATA_TYPE),
        "date" -> row.getAs[Long](DATE),
        "business_unit" -> row.getAs[String](BUSINESS_UNIT),
        "market_center_id" -> row.getAs[String](MARKET_CENTER_ID),
        "team_id" -> row.getAs[String](TEAM_ID),
        "user_type" -> row.getAs[String](USER_TYPE),
        "agent_id" -> row.getAs[String](AGENT_ID),
        "creator_id" -> row.getAs[String](CREATOR_ID),
        "source" -> row.getAs[String](SOURCE),
        "cid" -> row.getAs[String](CID),
        "direction" -> row.getAs[String](DIRECTION),
        "call_status" -> row.getAs[String](CALL_STATUS),
        "total_duration" -> row.getAs[Long](TOTAL_DURATION),
        "min_duration" -> row.getAs[Long](MIN_DURATION),
        "max_duration" -> row.getAs[Long](MAX_DURATION),
        "total_call" -> row.getAs[Long](TOTAL_CALL)
      )
    )
    JsonUtils.toJsonNode(jsonRow)
  }
}
