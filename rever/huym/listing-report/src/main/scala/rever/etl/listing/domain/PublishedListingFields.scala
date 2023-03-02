package rever.etl.listing.domain

object PublishedListingFields {
  final val TIMESTAMP = "timestamp"

  final val DATA_TYPE = "data_type"
  final val DATE = "date"
  final val LISTING_CHANNEL = "listing_channel"

  final val PUBLISH_TYPE = "publish_type"
  final val SERVICE_TYPE = "service_type"
  final val BUSINESS_UNIT = "business_unit"
  final val MARKET_CENTER_ID = "market_center_id"
  final val TEAM_ID = "team_id"
  final val CITY_ID = "city_id"
  final val DISTRICT_ID = "district_id"
  final val PROJECT_ID = "project_id"
  final val AGENT_ID = "agent_id"
  final val USER_TYPE = "user_type"
  final val PUBLISHER_ID = "publisher_id"
  final val CID = "cid"
  final val PROPERTY_ID = "property_id"

  final val TOTAL_LISTINGS = "total_listings"
  final val TOTAL_ACTIONS = "total_actions"

  // These following columns is used for intermediate computation only
  final val PUBLISHED_INFOS = "published_infos"
  final val MARKET_CENTER_NAME = "market_center_name"
  final val TEAM_NAME = "team_name"

}
