package rever.etl.data_sync.domain.engagement
object EmailDM {

  final val MESSAGE_ID = "email_message_id"
  final val DIRECTION = "email_direction"
  final val SCHEDULE_TYPE = "email_schedule_type"
  final val SENDER = "email_sender"
  final val FROM = "email_from"
  final val TO = "email_to"
  final val BCC = "email_bcc"
  final val CC = "email_cc"
  final val REPLY_TO = "email_reply_to"
  final val THREAD_ID = "email_thread_id"
  final val SUBJECT = "email_subject"
  final val TEXT = "email_text"
  final val HTML = "email_html"
  final val SEND_STATUS = "email_send_status"
  final val SEND_STATUS_MSG = "email_send_status_message"
  final val SEND_TIME = "email_send_time"
  final val TRACK_ID = "email_track_id"

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
    MESSAGE_ID,
    DIRECTION,
    SCHEDULE_TYPE,
    SENDER,
    FROM,
    TO,
    BCC,
    CC,
    REPLY_TO,
    THREAD_ID,
    SUBJECT,
    TEXT,
    HTML,
    SEND_STATUS,
    SEND_STATUS_MSG,
    SEND_TIME,
    TRACK_ID,
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
