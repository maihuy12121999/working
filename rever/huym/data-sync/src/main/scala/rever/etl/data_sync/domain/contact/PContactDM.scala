package rever.etl.data_sync.domain.contact
object PContactDM {
  final val TBL_NAME = "personal_contact"

  final val ID = "id"
  final val P_CID = "p_cid"
  final val CID = "cid"
  final val NICKNAME = "nickname"
  final val PERSONAL_TITLE = "personal_title"
  final val FIRST_NAME = "first_name"
  final val LAST_NAME = "last_name"
  final val FULL_NAME = "full_name"
  final val GENDER = "gender"
  final val PHONE = "phone"
  final val EMAIL = "email"
  final val ADDITIONAL_EMAILS = "additional_emails"
  final val AVATAR = "avatar"
  final val OWNER = "owner"
  final val OWNER_TEAM = "owner_team"
  final val OWNER_MC = "owner_market_center"
  final val ADMIN_P_CID = "admin_p_cid"
  final val CONTACT_SOURCE = "contact_source"
  final val CONTACT_STATUS = "contact_status"
  final val NOTES = "notes"
  final val PROPERTIES = "properties"
  final val CREATED_TIME = "created_time"
  final val UPDATED_TIME = "updated_time"
  final val STATUS = "status"
  final val DATA_HEALTH_SCORE = "data_health_score"
  final val DATA_HEALTH_INFO = "data_health_info"
  final val LOG_TIME = "log_time"

  final val PRIMARY_IDS = Seq(ID)

  val FIELDS = Seq(
    ID,
    P_CID,
    CID,
    NICKNAME,
    PERSONAL_TITLE,
    FIRST_NAME,
    LAST_NAME,
    FULL_NAME,
    GENDER,
    PHONE,
    EMAIL,
    ADDITIONAL_EMAILS,
    AVATAR,
    OWNER,
    OWNER_TEAM,
    OWNER_MC,
    ADMIN_P_CID,
    CONTACT_SOURCE,
    CONTACT_STATUS,
    NOTES,
    PROPERTIES,
    UPDATED_TIME,
    CREATED_TIME,
    STATUS,
    DATA_HEALTH_SCORE,
    DATA_HEALTH_INFO,
    LOG_TIME
  )
}
