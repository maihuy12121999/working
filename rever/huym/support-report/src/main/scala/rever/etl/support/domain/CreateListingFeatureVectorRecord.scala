package rever.etl.support.domain

case class CreateListingFeatureVectorRecord(property: Map[String, Any], listingType: String)

object CreateListingFeatureVectorRecord {
  final val PROPERTY_ID = "property_id"
  final val LISTING_TYPE = "listing_type"
  final val NUM_BED_ROOM = "num_bed_room"
  final val NUM_BATH_ROOM = "num_bath_room"
  final val AREA = "area"
  final val SERVICE_TYPE = "service_type"
  final val ADDRESS = "address"
  final val PROPERTY_TYPE = "property_type"
  final val IS_HOT = "is_hot"
  final val OWNERSHIP = "ownership"
  final val ARCHITECTURAL_STYLE = "architectural_style"
  final val DIRECTION = "direction"
  final val BALCONY_DIRECTION = "balcony_direction"
  final val ADDITIONAL_INFO = "additional_info"
  final val JURIDICAL_STATUS = "juridical_status"
  final val FURNITURE_STATUS = "furniture_status"
  final val HOUSE_STATUS = "house_status"
  final val ALLEYWAY_WIDTH = "alleyway_width"
  final val PROPERTY_STATUS = "property_status"
  final val SALE_DISCOUNT_VND = "sale_discount_vnd"
  final val RENT_DISCOUNT_VND = "rent_discount_vnd"
  final val PUBLISHED_TIME = "published_time"
  final val UPDATED_TIME = "updated_time"
  final val TRANSACTION = "transaction"
  final val SALE_PRICE_VND = "sale_price_vnd"
  final val RENT_PRICE_VND = "rent_price_vnd"

  final val LISTING_ID = "listing_id"
  final val FEATURE_VECTOR = "feature_vector"
  final val STAGE = "stage"
  final val MODEL_VERSION = "model_version"
  final val LOG_TIME = "log_time"

}
