package rever.etl.data_sync.domain.contact

object SysContactDM {
  final val TBL_NAME = "contact"

  final val ID = "id"
  final val CID = "cid"
  final val PERSONAL_TITLE = "personal_title"
  final val FIRST_NAME = "first_name"
  final val LAST_NAME = "last_name"
  final val FULL_NAME = "full_name"
  final val GENDER = "gender"
  final val PHONE = "phone"
  final val EMAILS = "emails"
  final val AVATAR = "avatar"
  final val CONTACT_SOURCE = "contact_source"
  final val C_CONTACT_SOURCE = "c_contact_source"
  final val HUBSPOT_IDS = "hubspot_ids"
  final val PROPERTIES = "properties"
  final val CREATED_BY = "created_by"
  final val UPDATED_BY = "updated_by"
  final val CREATED_TIME = "created_time"
  final val UPDATED_TIME = "updated_time"
  final val STATUS = "status"
  final val DATA_HEALTH_SCORE = "data_health_score"
  final val DATA_HEALTH_INFO = "data_health_info"
  final val LOG_TIME = "log_time"

  final val SEGMENT_TAGS = "c_segment_tags"

  final val PRIMARY_IDS = Seq(ID)

  val FIELDS = Seq(
    ID,
    CID,
    PERSONAL_TITLE,
    FIRST_NAME,
    LAST_NAME,
    FULL_NAME,
    GENDER,
    PHONE,
    EMAILS,
    AVATAR,
    CONTACT_SOURCE,
    C_CONTACT_SOURCE,
    HUBSPOT_IDS,
    SEGMENT_TAGS,
    PROPERTIES,
    CREATED_BY,
    UPDATED_BY,
    UPDATED_TIME,
    CREATED_TIME,
    STATUS,
    DATA_HEALTH_SCORE,
    DATA_HEALTH_INFO,
    LOG_TIME
  )
}
