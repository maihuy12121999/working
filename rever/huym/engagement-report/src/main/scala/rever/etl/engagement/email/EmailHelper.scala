package rever.etl.engagement.email

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.Row
import rever.etl.rsparkflow.utils.{JsonUtils, Utils}

object EmailHelper {

  final val DATA_TYPE = "data_type"

  final val TIMESTAMP = "timestamp"
  final val DATE = "date"

  final val BUSINESS_UNIT = "business_unit"
  final val MARKET_CENTER_ID = "market_center_id"
  final val TEAM_ID = "team_id"
  final val USER_TYPE = "user_type"
  final val AGENT_ID = "agent_id"
  final val CREATOR_ID = "creator_id"
  final val CONTACTS = "contacts"
  final val CIDS = "cids"
  final val CID = "cid"
  final val DIRECTION = "direction"
  final val EMAIL_STATUS = "email_status"
  final val EMAIL_ID = "email_id"
  final val TOTAL_EMAIL = "total_email"

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
        List(row.getAs[String](CID), "all"),
        List(row.getAs[String](DIRECTION)),
        List(row.getAs[String](EMAIL_STATUS)),
        List(row.getAs[String](EMAIL_ID))
      )
    )
    cartesianRows.flatMap(r => Seq(Row.fromSeq(r)))
  }

  def toNewEmailRecord(row: Row): JsonNode = {
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
        "cid" -> row.getAs[String](CID),
        "direction" -> row.getAs[String](DIRECTION),
        "email_status" -> row.getAs[String](EMAIL_STATUS),
        "total_email" -> row.getAs[Long](TOTAL_EMAIL)
      )
    )
    JsonUtils.toJsonNode(jsonRow)
  }
}
