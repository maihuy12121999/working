package rever.etl.data_sync.domain.engagement
object NoteDM {
  final val BODY = "note_body"

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
    BODY,
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
