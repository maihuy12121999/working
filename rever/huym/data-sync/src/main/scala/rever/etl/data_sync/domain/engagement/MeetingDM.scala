package rever.etl.data_sync.domain.engagement

object MeetingDM {

  final val INVITE_ID = "meeting_invite_id"
  final val INVITE_URL = "meeting_invite_url"
  final val ORGANIZER = "meeting_organizer"
  final val TITLE = "meeting_title"
  final val START_TIME = "meeting_start_time"
  final val END_TIME = "meeting_end_time"
  final val BODY = "meeting_body"
  final val INVITE_STATUS = "meeting_invite_status"
  final val INVITE_STATUS_MESSAGE = "meeting_invite_status_message"

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
    INVITE_ID,
    INVITE_URL,
    ORGANIZER,
    TITLE,
    START_TIME,
    END_TIME,
    BODY,
    INVITE_STATUS,
    INVITE_STATUS_MESSAGE,
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
