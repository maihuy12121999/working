package rever.etl.data_sync.domain.rever_search
object PropertyDM {
  final val PROPERTY_ID = "property_id"
  final val ALIAS = "alias"
  final val REVER_ID = "rever_id"
  final val NAME = "name"
  final val FULL_NAME = "full_name"
  final val SERVICE_TYPE = "service_type"
  final val PROPERTY_TYPE = "property_type"
  final val TRANSACTION = "transaction"
  final val SALE_DISCOUNT_VND = "sale_discount_vnd"
  final val RENT_DISCOUNT_VND = "rent_discount_vnd"
  final val SALE_PRICE_VND = "sale_price_vnd"
  final val RENT_PRICE_VND = "rent_price_vnd"
  final val NUM_BED_ROOM = "num_bed_room"
  final val NUM_BATH_ROOM = "num_bath_room"
  final val AREA = "area"
  final val AREA_USING = "area_using"
  final val AREA_BUILDING = "area_building"
  final val ALLEY_WAY_WIDTH = "alleyway_width"
  final val WIDTH = "width"
  final val LENGTH = "length"
  final val FLOORS = "floors"
  final val EXCLUSIVE = "exclusive"
  final val IS_HOT = "is_hot"
  final val PROJECT_ID = "project_id"
  final val ARCHITECTURAL_STYLE = "architectural_style"
  final val DIRECTION = "direction"
  final val BALCONY_DIRECTION = "balcony_direction"
  final val AMENITIES = "amenities"
  final val FURNITURE = "furniture"
  final val OWNERSHIP = "ownership"
  final val CONTACT = "contact"
  final val ADDRESS = "address"
  final val ADDITIONAL_INFO = "additional_info"
  final val OWNER_ID = "owner_id"
  final val TEAM_ID = "team_id"
  final val MARKET_CENTER_ID = "market_center_id"
  final val PROPERTY_NOTES = "property_notes"
  final val KEY_LOCK_NUMBER = "key_lock_number"
  final val LISTING_OPPO_ID = "listing_opportunity_id"
  final val FURNITURE_STATUS = "furniture_status"
  final val JURIDICAL_STATUS = "juridical_status"
  final val HOUSE_STATUS = "house_status"
  final val PROPERTY_STATUS = "property_status"
  final val PUBLISHED_STATUS = "published_status"
  final val SALE_STATUS = "sale_status"
  final val IS_VERIFIED = "is_verified"

  final val CONTENT_ASSIGNEE = "content_assignee"
  final val CONTENT_ASSIGNER = "content_assigner"
  final val CONTENT_STATUS = "content_status"
  final val REQUEST_CONTENT_TIME = "request_content_time"
  final val MEDIA_ASSIGNEE = "media_assignee"
  final val MEDIA_ASSIGNER = "media_assigner"
  final val MEDIA_STATUS = "media_status"

  final val CREATOR = "creator"
  final val CREATED_TIME = "created_time"
  final val UPDATED_TIME = "updated_time"
  final val PUBLISHED_TIME = "published_time"
  final val SIGNED_TIME = "signed_time"
  final val BUILDING_TIME = "building_time"

  final val DATA_HEALTH_SCORING_LEVEL = "data_health_scoring_level"
  final val DATA_HEALTH_SCORE = "data_health_score"
  final val DATA_HEALTH_INFO = "data_health_info"
  final val LOG_TIME = "log_time"

  final val PRIMARY_IDS = Seq(PROPERTY_ID)

  val FIELDS = Seq(
    PROPERTY_ID,
    ALIAS,
    REVER_ID,
    NAME,
    FULL_NAME,
    SERVICE_TYPE,
    PROPERTY_TYPE,
    TRANSACTION,
    SALE_DISCOUNT_VND,
    RENT_DISCOUNT_VND,
    SALE_PRICE_VND,
    RENT_PRICE_VND,
    NUM_BED_ROOM,
    NUM_BATH_ROOM,
    AREA,
    AREA_USING,
    AREA_BUILDING,
    ALLEY_WAY_WIDTH,
    WIDTH,
    LENGTH,
    FLOORS,
    EXCLUSIVE,
    IS_HOT,
    PROJECT_ID,
    ARCHITECTURAL_STYLE,
    DIRECTION,
    BALCONY_DIRECTION,
    AMENITIES,
    FURNITURE,
    OWNERSHIP,
    CONTACT,
    ADDRESS,
    ADDITIONAL_INFO,
    OWNER_ID,
    TEAM_ID,
    MARKET_CENTER_ID,
    PROPERTY_NOTES,
    KEY_LOCK_NUMBER,
    LISTING_OPPO_ID,
    FURNITURE_STATUS,
    JURIDICAL_STATUS,
    HOUSE_STATUS,
    PROPERTY_STATUS,
    PUBLISHED_STATUS,
    SALE_STATUS,
    IS_VERIFIED,
    CONTENT_ASSIGNEE,
    CONTENT_ASSIGNER,
    CONTENT_STATUS,
    REQUEST_CONTENT_TIME,
    MEDIA_ASSIGNEE,
    MEDIA_ASSIGNER,
    MEDIA_STATUS,
    CREATOR,
    UPDATED_TIME,
    CREATED_TIME,
    PUBLISHED_TIME,
    SIGNED_TIME,
    BUILDING_TIME,
    DATA_HEALTH_SCORING_LEVEL,
    DATA_HEALTH_SCORE,
    DATA_HEALTH_INFO,
    LOG_TIME
  )

}

object Property {

  final val PROPERTY_ID = "property_id"
  final val ALIAS = "alias"
  final val REVER_ID = "rever_id"
  final val NAME = "name"
  final val FULL_NAME = "full_name"
  final val SERVICE_TYPE = "service_type"
  final val PROPERTY_TYPE = "property_type"
  final val TRANSACTION = "transaction"
  final val SALE_DISCOUNT_VND = "sale_discount_vnd"
  final val RENT_DISCOUNT_VND = "rent_discount_vnd"
  final val NUM_BED_ROOM = "num_bed_room"
  final val NUM_BATH_ROOM = "num_bath_room"
  final val AREA = "area"
  final val AREA_USING = "area_using"
  final val AREA_BUILDING = "area_building"
  final val ALLEY_WAY_WIDTH = "alleyway_width"
  final val WIDTH = "width"
  final val LENGTH = "length"
  final val FLOORS = "floors"
  final val EXCLUSIVE = "exclusive"
  final val IS_HOT = "is_hot"
  final val PROJECT_ID = "project_id"
  final val ARCHITECTURAL_STYLE = "architectural_style"
  final val DIRECTION = "direction"
  final val BALCONY_DIRECTION = "balcony_direction"
  final val AMENITIES = "amenities"
  final val FURNITURE = "furniture"
  final val OWNERSHIP = "ownership"
  final val CONTACT = "contact"
  final val ADDRESS = "address"
  final val ADDITIONAL_INFO = "additional_info"
  final val OWNER_ID = "owner_id"
  final val TEAM_ID = "team_id"
  final val MARKET_CENTER_ID = "market_center_id"
  final val PROPERTY_NOTES = "property_notes"
  final val KEY_LOCK_NUMBER = "key_lock_number"
  final val LISTING_OPPO_ID = "listing_opportunity_id"
  final val FURNITURE_STATUS = "furniture_status"
  final val JURIDICAL_STATUS = "juridical_status"
  final val HOUSE_STATUS = "house_status"
  final val PROPERTY_STATUS = "property_status"
  final val SALE_STATUS = "sale_status"

  final val CONTENT_ASSIGNEE = "content_assignee"
  final val CONTENT_ASSIGNER = "content_assigner"
  final val CONTENT_STATUS = "content_status"
  final val REQUEST_CONTENT_TIME = "request_content_time"
  final val MEDIA_ASSIGNEE = "media_assignee"
  final val MEDIA_ASSIGNER = "media_assigner"
  final val MEDIA_STATUS = "media_status"

  final val CREATOR = "creator"
  final val CREATED_TIME = "created_time"
  final val UPDATED_TIME = "updated_time"
  final val PUBLISHED_TIME = "published_time"
  final val SIGNED_TIME = "signed_time"
  final val BUILDING_TIME = "building_time"

  final val PRIMARY_IDS = Seq(PROPERTY_ID)

  val FIELDS = Seq(
    PROPERTY_ID,
    ALIAS,
    REVER_ID,
    NAME,
    FULL_NAME,
    SERVICE_TYPE,
    PROPERTY_TYPE,
    TRANSACTION,
    SALE_DISCOUNT_VND,
    RENT_DISCOUNT_VND,
    NUM_BED_ROOM,
    NUM_BATH_ROOM,
    AREA,
    AREA_USING,
    AREA_BUILDING,
    ALLEY_WAY_WIDTH,
    WIDTH,
    LENGTH,
    FLOORS,
    EXCLUSIVE,
    IS_HOT,
    PROJECT_ID,
    ARCHITECTURAL_STYLE,
    DIRECTION,
    BALCONY_DIRECTION,
    AMENITIES,
    FURNITURE,
    OWNERSHIP,
    CONTACT,
    ADDRESS,
    ADDITIONAL_INFO,
    OWNER_ID,
    TEAM_ID,
    MARKET_CENTER_ID,
    PROPERTY_NOTES,
    KEY_LOCK_NUMBER,
    LISTING_OPPO_ID,
    FURNITURE_STATUS,
    JURIDICAL_STATUS,
    HOUSE_STATUS,
    PROPERTY_STATUS,
    SALE_STATUS,
    CONTENT_ASSIGNEE,
    CONTENT_ASSIGNER,
    CONTENT_STATUS,
    REQUEST_CONTENT_TIME,
    MEDIA_ASSIGNEE,
    MEDIA_ASSIGNER,
    MEDIA_STATUS,
    CREATOR,
    UPDATED_TIME,
    CREATED_TIME,
    PUBLISHED_TIME,
    SIGNED_TIME,
    BUILDING_TIME
  )

}
