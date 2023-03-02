package rever.etl.data_sync.domain.engagement
object CallDM {
  final val SYSTEM_ID = "call_system_id"
  final val SYSTEM_EXTENSION = "call_system_extension"
  final val SYSTEM_USER = "call_system_user"
  final val DIRECTION = "call_direction"
  final val DIRECTION_NAME = "call_direction_name"
  final val FROM_PHONE = "call_from_phone"
  final val TO_PHONE = "call_to_phone"
  final val FROM_PHONE_NORMALIZED = "call_from_phone_normalized"
  final val TO_PHONE_NORMALIZED = "call_to_phone_normalized"
  final val BODY = "call_body"
  final val DURATION = "call_duration"
  final val RECORDING_URL = "call_recording_url"
  final val STATUS = "call_status"
  final val STATUS_NAME = "call_status_name"

  final val PRIMARY_IDS = Seq(EngagementDM.ID)

  val FIELDS = Seq(
    EngagementDM.ID,
    EngagementDM.ACTIVITY_TYPE,
    EngagementDM.SCOPE,
    EngagementDM.TYPE,
    EngagementDM.SOURCE,
    EngagementDM.SOURCE_ID,
    EngagementDM.ATTACHMENTS,
    EngagementDM.ASSOCIATION_ENGAGEMENT_IDS,
    EngagementDM.ASSOCIATION_OPPORTUNITY_IDS,
    EngagementDM.ASSOCIATION_INQUIRY_IDS,
    EngagementDM.ASSOCIATION_PROJECT_IDS,
    EngagementDM.ASSOCIATION_PROPERTY_IDS,
    EngagementDM.ASSOCIATION_MLS_PROPERTY_IDS,
    EngagementDM.ASSOCIATION_TICKET_IDS,
    EngagementDM.ASSOCIATION_OPERATORS,
    EngagementDM.ASSOCIATION_CONTACTS,
    EngagementDM.ASSOCIATION_CONTACT_CIDS,
    EngagementDM.ASSOCIATION_CONTACT_P_CIDS,
    EngagementDM.ASSOCIATION_VISITORS,
    SYSTEM_ID,
    SYSTEM_EXTENSION,
    SYSTEM_USER,
    DIRECTION,
    DIRECTION_NAME,
    FROM_PHONE,
    TO_PHONE,
    FROM_PHONE_NORMALIZED,
    TO_PHONE_NORMALIZED,
    BODY,
    DURATION,
    RECORDING_URL,
    STATUS,
    STATUS_NAME,
    EngagementDM.OWNER_ID,
    EngagementDM.TEAM_ID,
    EngagementDM.MARKET_CENTER_ID,
    EngagementDM.UPDATED_BY,
    EngagementDM.CREATED_BY,
    EngagementDM.UPDATED_TIME,
    EngagementDM.CREATED_TIME,
    EngagementDM.TIMESTAMP,
    EngagementDM.STATUS,
    EngagementDM.LOG_TIME
  )

}
