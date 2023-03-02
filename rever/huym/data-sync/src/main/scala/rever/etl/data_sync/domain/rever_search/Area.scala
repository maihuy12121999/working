package rever.etl.data_sync.domain.rever_search

case class Point(lat:Double, lon:Double)

object Area {

  final val AREA_ID = "area_id"
  final val AREA_TYPE = "area_type"
  final val AREA_LEVEL = "area_level"
  final val ALIAS = "alias"
  final val NAME = "name"
  final val FULL_NAME = "full_name"
  final val KEYWORD = "keyword"
  final val ADDRESS = "address"
  final val ADDITIONAL_INFO = "additional_info"
  final val CENTRAL_POINT = "central_point"
  final val CREATOR = "creator"
  final val UPDATED_BY = "updated_by"
  final val CREATED_TIME = "created_time"
  final val UPDATED_TIME = "updated_time"
  final val STATUS = "status"

  final val PRIMARY_IDS = Seq(AREA_ID)

  val FIELDS = Seq(
    AREA_ID,
    AREA_TYPE,
    AREA_LEVEL,
    ALIAS,
    NAME,
    FULL_NAME,
    KEYWORD,
    ADDRESS,
    ADDITIONAL_INFO,
    CENTRAL_POINT,
    CREATOR,
    UPDATED_BY,
    UPDATED_TIME,
    CREATED_TIME,
    STATUS
  )

}
