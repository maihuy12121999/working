package rever.etl.data_sync.domain.rever_search
object InquiryDM {

  final val ID = "id"
  final val INQUIRY_CODE = "inquiry_code"
  final val NAME = "name"
  final val DESCRIPTION = "description"
  final val SERVICE_TYPE = "service_type"
  final val PROPERTY_TYPES = "property_types"
  final val CID = "cid"
  final val P_CID = "p_cid"
  final val MIN_PRICE = "min_price"
  final val MAX_PRICE = "max_price"
  final val MIN_AREA = "min_area"
  final val MAX_AREA = "max_area"
  final val MIN_BED = "min_bed"
  final val MAX_BED = "max_bed"
  final val MIN_TOILET = "min_toilet"
  final val MAX_TOILET = "max_toilet"
  final val DIRECTIONS = "directions"
  final val BALCONY_DIRECTIONS = "balcony_directions"
  final val ADDRESS = "address"
  final val OWNER_ID = "owner"
  final val TEAM_ID = "owner_team"
  final val MARKET_CENTER_ID = "owner_mc"
  final val CREATED_BY = "created_by"
  final val UPDATED_BY = "updated_by"
  final val CREATED_TIME = "created_time"
  final val UPDATED_TIME = "updated_time"
  final val STATUS = "status"
  final val VERIFIED = "verified"
  final val LOG_TIME = "log_time"

  final val PRIMARY_IDS = Seq(ID)

  val FIELDS = Seq(
    ID,
    INQUIRY_CODE,
    NAME,
    DESCRIPTION,
    SERVICE_TYPE,
    PROPERTY_TYPES,
    CID,
    P_CID,
    MIN_PRICE,
    MAX_PRICE,
    MIN_AREA,
    MAX_AREA,
    MIN_BED,
    MAX_BED,
    MIN_TOILET,
    MAX_TOILET,
    DIRECTIONS,
    BALCONY_DIRECTIONS,
    ADDRESS,
    OWNER_ID,
    TEAM_ID,
    MARKET_CENTER_ID,
    CREATED_BY,
    UPDATED_BY,
    UPDATED_TIME,
    CREATED_TIME,
    STATUS,
    VERIFIED
  )

}
