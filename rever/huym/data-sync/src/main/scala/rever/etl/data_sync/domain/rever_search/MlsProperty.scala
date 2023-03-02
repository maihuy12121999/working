package rever.etl.data_sync.domain.rever_search

object MlsPropertyDM {

  final val ID = "id"
  final val MLS_ID = "mls_id"
  final val NAME = "name"
  final val SERVICE_TYPE = "service_type"
  final val PROPERTY_TYPE = "property_type"
  final val NUM_BED_ROOM = "num_bed_room"
  final val NUM_BATH_ROOM = "num_bath_room"
  final val AREA = "area"
  final val AREA_USING = "area_using"
  final val WIDTH = "width"
  final val LENGTH = "length"
  final val TOTAL_FLOORS = "total_floors"
  final val DIRECTION = "direction"
  final val SELL_PRICE_VND = "sell_price_vnd"
  final val PROJECT_NAME = "project_name"
  final val ADDRESS = "address"
  final val CONTACT = "contact"
  final val MEDIA = "media"
  final val REVIEW = "review"
  final val MARKETING = "marketing"
  final val ORIGINAL_INFO = "original_info"
  final val CRAWLED_TIME = "crawled_time"
  final val CREATED_TIME = "created_time"
  final val UPDATED_TIME = "updated_time"
  final val UPDATED_BY = "updated_by"
  final val STATUS = "status"
  final val ASSIGNEE = "assignee"
  final val SCORE_INFO = "score_info"
  final val LOG_TIME = "log_time"

  final val PRIMARY_IDS = Seq(ID)

  val FIELDS = Seq(
    ID,
    MLS_ID,
    NAME,
    SERVICE_TYPE,
    PROPERTY_TYPE,
    NUM_BED_ROOM,
    NUM_BATH_ROOM,
    AREA,
    AREA_USING,
    WIDTH,
    LENGTH,
    TOTAL_FLOORS,
    DIRECTION,
    SELL_PRICE_VND,
    PROJECT_NAME,
    ADDRESS,
    CONTACT,
    MEDIA,
    REVIEW,
    MARKETING,
    CRAWLED_TIME,
    UPDATED_TIME,
    CREATED_TIME,
    UPDATED_BY,
    STATUS,
    ASSIGNEE,
    SCORE_INFO,
    LOG_TIME
  )

}

object MlsProperty {

  final val ID = "id"
  final val MLS_ID = "mls_id"
  final val NAME = "name"
  final val SERVICE_TYPE = "service_type"
  final val PROPERTY_TYPE = "property_type"
  final val NUM_BED_ROOM = "num_bed_room"
  final val NUM_BATH_ROOM = "num_bath_room"
  final val AREA = "area"
  final val AREA_USING = "area_using"
  final val WIDTH = "width"
  final val LENGTH = "length"
  final val TOTAL_FLOORS = "total_floors"
  final val DIRECTION = "direction"
  final val SELL_PRICE_VND = "sell_price_vnd"
  final val PROJECT_NAME = "project_name"
  final val ADDRESS = "address"
  final val CONTACT = "contact"
  final val MEDIA = "media"
  final val REVIEW = "review"
  final val MARKETING = "marketing"
  final val ORIGINAL_INFO = "original_info"
  final val CRAWLED_TIME = "crawled_time"
  final val CREATED_TIME = "created_time"
  final val UPDATED_TIME = "updated_time"
  final val UPDATED_BY = "updated_by"
  final val STATUS = "status"
  final val ASSIGNEE = "assignee"
  final val SCORE_INFO = "score_info"

  final val PRIMARY_IDS = Seq(ID)

  val FIELDS = Seq(
    ID,
    MLS_ID,
    NAME,
    SERVICE_TYPE,
    PROPERTY_TYPE,
    NUM_BED_ROOM,
    NUM_BATH_ROOM,
    AREA,
    AREA_USING,
    WIDTH,
    LENGTH,
    TOTAL_FLOORS,
    DIRECTION,
    SELL_PRICE_VND,
    PROJECT_NAME,
    ADDRESS,
    CONTACT,
    MEDIA,
    REVIEW,
    MARKETING,
    CRAWLED_TIME,
    UPDATED_TIME,
    CREATED_TIME,
    UPDATED_BY,
    STATUS,
    ASSIGNEE,
    SCORE_INFO
  )

}
