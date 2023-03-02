package rever.etl.listing.published_listings.writer

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{Dataset, Row}
import rever.etl.listing.domain.PublishedListingFields
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.extensions.RapIngestWriterMixin
import rever.etl.rsparkflow.utils.JsonUtils

class PublishedListingWriter extends SinkWriter with RapIngestWriterMixin {

  override def write(s: String, df: Dataset[Row], config: Config): Dataset[Row] = {

    val topic = config.get("published_listing_topic")
    ingestDataframe(config, df, topic)(toPublishedListingRecord)
    df
  }
  private def toPublishedListingRecord(row: Row): JsonNode = {
    val recordMap = Map(
      PublishedListingFields.DATA_TYPE -> row.getAs[String](PublishedListingFields.DATA_TYPE),
      PublishedListingFields.DATE -> row.getAs[Long](PublishedListingFields.DATE),
      PublishedListingFields.LISTING_CHANNEL -> row.getAs[String](PublishedListingFields.LISTING_CHANNEL),
      PublishedListingFields.PUBLISH_TYPE -> row.getAs[String](PublishedListingFields.PUBLISH_TYPE),
      PublishedListingFields.SERVICE_TYPE -> row.getAs[String](PublishedListingFields.SERVICE_TYPE),
      PublishedListingFields.BUSINESS_UNIT -> row.getAs[String](PublishedListingFields.BUSINESS_UNIT),
      PublishedListingFields.MARKET_CENTER_ID -> row.getAs[String](PublishedListingFields.MARKET_CENTER_ID),
      PublishedListingFields.TEAM_ID -> row.getAs[String](PublishedListingFields.TEAM_ID),
      PublishedListingFields.CITY_ID -> row.getAs[String](PublishedListingFields.CITY_ID),
      PublishedListingFields.DISTRICT_ID -> row.getAs[String](PublishedListingFields.DISTRICT_ID),
      PublishedListingFields.PROJECT_ID -> row.getAs[String](PublishedListingFields.PROJECT_ID),
      PublishedListingFields.AGENT_ID -> row.getAs[String](PublishedListingFields.AGENT_ID),
      PublishedListingFields.USER_TYPE -> row.getAs[String](PublishedListingFields.USER_TYPE),
      PublishedListingFields.PUBLISHER_ID -> row.getAs[String](PublishedListingFields.PUBLISHER_ID),
      PublishedListingFields.CID -> row.getAs[String](PublishedListingFields.CID),
      PublishedListingFields.TOTAL_LISTINGS -> row.getAs[Long](PublishedListingFields.TOTAL_LISTINGS),
      PublishedListingFields.TOTAL_ACTIONS -> row.getAs[Long](PublishedListingFields.TOTAL_ACTIONS)
    )
    JsonUtils.toJsonNode(JsonUtils.toJson(recordMap))
  }
}
