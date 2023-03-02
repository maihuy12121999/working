package rever.etl.listing.new_total_listing

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.Row
import rever.etl.rsparkflow.utils.{JsonUtils, Utils}

object EmcListingHelper {

  final val DATA_TYPE = "data_type"
  final val TIMESTAMP = "timestamp"
  final val DATE = "date"

  final val LISTING_CHANNEL = "listing_channel"
  final val BUSINESS_UNIT = "business_unit"
  final val MARKET_CENTER_ID = "market_center_id"
  final val MARKET_CENTER_NAME = "market_center_name"
  final val TEAM_ID = "team_id"
  final val TEAM_NAME = "team_name"
  final val PROJECT_ID = "project_id"
  final val AGENT_ID = "agent_id"
  final val LISTING_ID = "listing_id"
  final val CITY_ID = "city_id"
  final val DISTRICT_ID = "district_id"
  final val SERVICE_TYPE = "service_type"
  final val PUBLISHED_ID = "published_id"
  final val PROPERTY_STATUS = "property_status"
  final val PROPERTY_TRANSACTION_STAGE = "property_transaction_stage"
  final val TOTAL_LISTING = "total_listing"

  def generateRow(row: Row): Seq[Row] = {

    val cartesianRows = Utils.cartesianProduct(
      List(
        List(row.getAs[Long](DATE)),
        List(row.getAs[String](LISTING_CHANNEL)),
        List(row.getAs[String](BUSINESS_UNIT), "all"),
        List(row.getAs[String](MARKET_CENTER_ID), "all"),
        List(row.getAs[String](TEAM_ID), "all"),
        List(row.getAs[String](CITY_ID), "all"),
        List(row.getAs[String](DISTRICT_ID), "all"),
        List(row.getAs[String](PROJECT_ID)),
        List(row.getAs[String](AGENT_ID)),
        List(row.getAs[String](PUBLISHED_ID)),
        List(row.getAs[String](SERVICE_TYPE), "all"),
        List(row.getAs[String](PROPERTY_STATUS)),
        List(row.getAs[String](PROPERTY_TRANSACTION_STAGE)),
        List(row.getAs[String](LISTING_ID))
      )
    )
    cartesianRows.flatMap(r => Seq(Row.fromSeq(r)))
  }

  def toNewListingRecord(row: Row): JsonNode = {
    val jsonRow = JsonUtils.toJson(
      Map(
        "data_type" -> row.getAs[String](DATA_TYPE),
        "date" -> row.getAs[Long](DATE),
        "listing_channel" -> row.getAs[String](LISTING_CHANNEL),
        "business_unit" -> row.getAs[String](BUSINESS_UNIT),
        "market_center_id" -> row.getAs[String](MARKET_CENTER_ID),
        "team_id" -> row.getAs[String](TEAM_ID),
        "city_id" -> row.getAs[String](CITY_ID),
        "district_id" -> row.getAs[String](DISTRICT_ID),
        "project_id" -> row.getAs[String](PROJECT_ID),
        "agent_id" -> row.getAs[String](AGENT_ID),
        "published_id" -> row.getAs[String](PUBLISHED_ID),
        "service_type" -> row.getAs[String](SERVICE_TYPE),
        "property_status" -> row.getAs[String](PROPERTY_STATUS),
        "property_transaction_stage" -> row.getAs[String](PROPERTY_TRANSACTION_STAGE),
        "total_listing" -> row.getAs[Long](TOTAL_LISTING)
      )
    )
    JsonUtils.toJsonNode(jsonRow)
  }

  def toTotalListingRecord(row: Row): JsonNode = {
    val jsonRow = JsonUtils.toJson(
      Map(
        "data_type" -> row.getAs[String](DATA_TYPE),
        "date" -> row.getAs[Long](DATE),
        "listing_channel" -> row.getAs[String](LISTING_CHANNEL),
        "business_unit" -> row.getAs[String](BUSINESS_UNIT),
        "market_center_id" -> row.getAs[String](MARKET_CENTER_ID),
        "team_id" -> row.getAs[String](TEAM_ID),
        "city_id" -> row.getAs[String](CITY_ID),
        "district_id" -> row.getAs[String](DISTRICT_ID),
        "project_id" -> row.getAs[String](PROJECT_ID),
        "agent_id" -> row.getAs[String](AGENT_ID),
        "published_id" -> row.getAs[String](PUBLISHED_ID),
        "service_type" -> row.getAs[String](SERVICE_TYPE),
        "property_status" -> row.getAs[String](PROPERTY_STATUS),
        "property_transaction_stage" -> row.getAs[String](PROPERTY_TRANSACTION_STAGE),
        "total_listing" -> row.getAs[Long](TOTAL_LISTING)
      )
    )
    JsonUtils.toJsonNode(jsonRow)
  }
}
