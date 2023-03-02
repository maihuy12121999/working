package rever.etl.data_sync.normalizer

import com.fasterxml.jackson.databind.JsonNode
import org.elasticsearch.search.SearchHit
import rever.etl.data_sync.core.Normalizer
import rever.etl.data_sync.domain.engagement._
import rever.etl.rsparkflow.utils.JsonUtils
import vn.rever.common.util.PhoneUtils

/** @author anhlt (andy)
  */

case class EngagementNormalizer(engagementType: String) extends Normalizer[SearchHit, Map[String, Any]] {

  override def toRecord(searchHit: SearchHit, total: Long): Option[Map[String, Any]] = {

    val reverEngagement = JsonUtils.fromJson[ReverEngagement](searchHit.getSourceAsString)

    val id = searchHit.getId

    val (cids, pCids) = reverEngagement.associations.map(_.getCidAndPCidList).getOrElse((Seq.empty, Seq.empty))

    val dataMap = Map(
      EngagementDM.ID -> id,
      EngagementDM.ACTIVITY_TYPE -> reverEngagement.activityType.getOrElse(""),
      EngagementDM.SCOPE -> reverEngagement.scope.getOrElse(0),
      EngagementDM.TYPE -> reverEngagement.`type`.getOrElse(""),
      EngagementDM.SOURCE -> reverEngagement.source.getOrElse(""),
      EngagementDM.SOURCE_ID -> reverEngagement.sourceId.getOrElse(""),
      EngagementDM.ATTACHMENTS -> reverEngagement.attachments.getOrElse(Seq.empty).map(_.toString).toArray,
      EngagementDM.ASSOCIATION_ENGAGEMENT_IDS -> reverEngagement.associations
        .flatMap(_.engagementIds)
        .getOrElse(Seq.empty)
        .toArray,
      EngagementDM.ASSOCIATION_OPPORTUNITY_IDS -> reverEngagement.associations
        .flatMap(_.opportunityIds)
        .getOrElse(Seq.empty)
        .toArray,
      EngagementDM.ASSOCIATION_INQUIRY_IDS -> reverEngagement.associations
        .flatMap(_.inquiryIds)
        .getOrElse(Seq.empty)
        .toArray,
      EngagementDM.ASSOCIATION_PROJECT_IDS -> reverEngagement.associations
        .flatMap(_.projectIds)
        .getOrElse(Seq.empty)
        .toArray,
      EngagementDM.ASSOCIATION_PROPERTY_IDS -> reverEngagement.associations
        .flatMap(_.propertyIds)
        .getOrElse(Seq.empty)
        .toArray,
      EngagementDM.ASSOCIATION_MLS_PROPERTY_IDS -> reverEngagement.associations
        .flatMap(_.mlsPropertyIds)
        .getOrElse(Seq.empty)
        .toArray,
      EngagementDM.ASSOCIATION_TICKET_IDS -> reverEngagement.associations
        .flatMap(_.ticketIds)
        .getOrElse(Seq.empty)
        .toArray,
      EngagementDM.ASSOCIATION_OPERATORS -> reverEngagement.associations
        .flatMap(_.operators)
        .getOrElse(Seq.empty)
        .map(_.toString)
        .toArray,
      EngagementDM.ASSOCIATION_CONTACTS -> reverEngagement.associations
        .flatMap(_.contacts)
        .getOrElse(Seq.empty)
        .map(_.toString)
        .toArray,
      EngagementDM.ASSOCIATION_CONTACT_CIDS -> cids.toArray,
      EngagementDM.ASSOCIATION_CONTACT_P_CIDS -> pCids.toArray,
      EngagementDM.ASSOCIATION_VISITORS -> reverEngagement.associations
        .flatMap(_.visitors)
        .getOrElse(Seq.empty)
        .map(_.toString)
        .toArray,
      EngagementDM.OWNER_ID -> reverEngagement.owner.getOrElse(""),
      EngagementDM.TEAM_ID -> reverEngagement.ownerTeam.getOrElse(""),
      EngagementDM.MARKET_CENTER_ID -> reverEngagement.ownerMarketCenter.getOrElse(""),
      EngagementDM.CREATED_BY -> reverEngagement.createdBy.getOrElse(""),
      EngagementDM.UPDATED_BY -> reverEngagement.updatedBy.getOrElse(""),
      EngagementDM.CREATED_TIME -> reverEngagement.createdTime.getOrElse(0L),
      EngagementDM.UPDATED_TIME -> reverEngagement.updatedTime.getOrElse(0L),
      EngagementDM.TIMESTAMP -> reverEngagement.timestamp.getOrElse(0L),
      EngagementDM.STATUS -> reverEngagement.status.getOrElse(0),
      EngagementDM.LOG_TIME -> System.currentTimeMillis()
    )

    val metadataMap = engagementType match {
      case EngagementTypes.CALL =>
        reverEngagement.callMetadata.map(_.toCHDataMap).getOrElse(Map.empty[String, Any])
      case EngagementTypes.EMAIL =>
        reverEngagement.emailMetadata.map(_.toCHDataMap).getOrElse(Map.empty[String, Any])
      case EngagementTypes.MEETING =>
        reverEngagement.meetingMetadata.map(_.toCHDataMap).getOrElse(Map.empty[String, Any])
      case EngagementTypes.NOTE =>
        reverEngagement.noteMetadata.map(_.toCHDataMap).getOrElse(Map.empty[String, Any])
      case EngagementTypes.SURVEY =>
        reverEngagement.surveyMetadata.map(_.toCHDataMap).getOrElse(Map.empty[String, Any])
      case _ => Map.empty[String, Any]
    }

    Some(dataMap ++ metadataMap)
  }

}

private[this] case class ReverEngagement(
    id: Option[String],
    activityType: Option[String],
    scope: Option[Int],
    `type`: Option[String],
    source: Option[String],
    sourceId: Option[String],
    attachments: Option[Seq[JsonNode]],
    associations: Option[Association],
    callMetadata: Option[CallMetadata],
    emailMetadata: Option[EmailMetadata],
    meetingMetadata: Option[MeetingMetadata],
    noteMetadata: Option[NoteMetadata],
    surveyMetadata: Option[SurveyMetadata],
    owner: Option[String],
    ownerTeam: Option[String],
    ownerMarketCenter: Option[String],
    createdBy: Option[String],
    updatedBy: Option[String],
    createdTime: Option[Long],
    updatedTime: Option[Long],
    timestamp: Option[Long],
    status: Option[Int]
)

private[this] case class Association(
    contacts: Option[Seq[JsonNode]],
    engagementIds: Option[Seq[String]],
    opportunityIds: Option[Seq[String]],
    inquiryIds: Option[Seq[String]],
    projectIds: Option[Seq[String]],
    propertyIds: Option[Seq[String]],
    mlsPropertyIds: Option[Seq[String]],
    ticketIds: Option[Seq[String]],
    operators: Option[Seq[JsonNode]],
    users: Option[Seq[JsonNode]],
    visitors: Option[Seq[JsonNode]]
) {

  def getCidAndPCidList: (Seq[String], Seq[String]) = {
    val ids = contacts
      .getOrElse(Seq.empty)
      .map(node => {
        val cid = node.at("/cid").asText("")
        val pCid = node.at("/p_cid").asText("")
        cid -> pCid
      })
      .filterNot(pair => pair._1 == null && pair._2 == null)
      .filterNot(pair => pair._1.isEmpty && pair._2.isEmpty)

    (ids.map(_._1), ids.map(_._2))
  }
}

private[this] case class CallMetadata(
    systemId: Option[String],
    systemExtension: Option[String],
    systemUser: Option[String],
    direction: Option[Int],
    fromPhone: Option[String],
    toPhone: Option[String],
    body: Option[String],
    duration: Option[Int],
    recordingUrl: Option[String],
    status: Option[Int]
) {

  def toCHDataMap: Map[String, Any] = {

    Map(
      CallDM.SYSTEM_ID -> systemId.getOrElse(""),
      CallDM.SYSTEM_EXTENSION -> systemExtension.getOrElse(""),
      CallDM.SYSTEM_USER -> systemUser.getOrElse(""),
      CallDM.DIRECTION -> direction.getOrElse(0),
      CallDM.DIRECTION_NAME -> toCallDirectionName(direction.getOrElse(0)),
      CallDM.FROM_PHONE -> fromPhone.getOrElse(""),
      CallDM.TO_PHONE -> toPhone.getOrElse(""),
      CallDM.FROM_PHONE_NORMALIZED -> PhoneUtils.normalizePhone(fromPhone.getOrElse("")).getOrElse(""),
      CallDM.TO_PHONE_NORMALIZED -> PhoneUtils.normalizePhone(toPhone.getOrElse("")).getOrElse(""),
      CallDM.BODY -> body.getOrElse(""),
      CallDM.DURATION -> duration.getOrElse(0),
      CallDM.RECORDING_URL -> recordingUrl.getOrElse(""),
      CallDM.STATUS -> status.getOrElse(0),
      CallDM.STATUS_NAME -> toCallStatusName(status.getOrElse(0))
    )
  }

  private def toCallDirectionName(callDirection: Int): String = {
    callDirection match {
      case 0 => "other"
      case 1 => "inbound"
      case 2 => "outbound"
      case _ => throw new Exception(s"Unknown call direction: ${callDirection}")
    }
  }

  private def toCallStatusName(callStatus: Int): String = {
    callStatus match {
      case 0 => "other"
      case 1 => "connected"
      case 2 => "no_answer"
      case 3 => "busy"
      case 4 => "canceled"
      case 5 => "wrong_number"
      case 6 => "unavailable"
      case 7 => "server_denied"
      case _ => throw new Exception(s"Unknown call status: ${callStatus}")
    }
  }

}

private[this] case class EmailMetadata(
    messageId: Option[String],
    scheduleType: Option[Int],
    sender: Option[JsonNode],
    direction: Option[String],
    from: Option[JsonNode],
    to: Option[JsonNode],
    bcc: Option[JsonNode],
    cc: Option[JsonNode],
    replyTo: Option[String],
    threadId: Option[String],
    subject: Option[String],
    text: Option[String],
    html: Option[String],
    sendStatus: Option[Int],
    sendStatusMessage: Option[String],
    sendTime: Option[Long],
    trackId: Option[String]
) {
  def toCHDataMap: Map[String, Any] = {
    Map(
      EmailDM.MESSAGE_ID -> messageId.getOrElse(""),
      EmailDM.DIRECTION -> direction.getOrElse(""),
      EmailDM.SCHEDULE_TYPE -> scheduleType.getOrElse(0),
      EmailDM.SENDER -> sender.map(_.toString).getOrElse("{}"),
      EmailDM.FROM -> from.map(_.toString).getOrElse("{}"),
      EmailDM.TO -> to.map(_.toString).getOrElse("{}"),
      EmailDM.BCC -> bcc.map(_.toString).getOrElse("{}"),
      EmailDM.CC -> cc.map(_.toString).getOrElse("{}"),
      EmailDM.REPLY_TO -> replyTo.getOrElse(""),
      EmailDM.THREAD_ID -> threadId.getOrElse(""),
      EmailDM.SUBJECT -> subject.getOrElse(""),
      EmailDM.TEXT -> text.getOrElse(""),
      EmailDM.HTML -> html.getOrElse(""),
      EmailDM.SEND_STATUS -> sendStatus.getOrElse(0),
      EmailDM.SEND_STATUS_MSG -> sendStatusMessage.getOrElse(""),
      EmailDM.SEND_TIME -> sendTime.getOrElse(0L),
      EmailDM.TRACK_ID -> trackId.getOrElse("")
    )
  }
}

private[this] case class MeetingMetadata(
    inviteId: Option[String],
    inviteUrl: Option[String],
    organizer: Option[JsonNode],
    title: Option[String],
    startTime: Option[Long],
    endTime: Option[Long],
    body: Option[String],
    inviteStatus: Option[Int],
    inviteStatusMessage: Option[String]
) {
  def toCHDataMap: Map[String, Any] = {
    Map(
      MeetingDM.INVITE_ID -> inviteId.getOrElse(""),
      MeetingDM.INVITE_URL -> inviteUrl.getOrElse(""),
      MeetingDM.ORGANIZER -> organizer.map(_.toString).getOrElse("{}"),
      MeetingDM.TITLE -> title.getOrElse(""),
      MeetingDM.START_TIME -> startTime.getOrElse(0L),
      MeetingDM.END_TIME -> endTime.getOrElse(0L),
      MeetingDM.BODY -> body.getOrElse(""),
      MeetingDM.INVITE_STATUS -> inviteStatus.getOrElse(0),
      MeetingDM.INVITE_STATUS_MESSAGE -> inviteStatusMessage.getOrElse("")
    )
  }
}

private[this] case class NoteMetadata(body: Option[String]) {

  def toCHDataMap: Map[String, Any] = {
    Map(
      NoteDM.BODY -> body.getOrElse("")
    )
  }
}

private[this] case class SurveyMetadata(
    templateId: Option[String],
    channel: Option[String],
    answers: Option[JsonNode],
    searchMetadata: Option[JsonNode]
) {
  def toCHDataMap: Map[String, Any] = {

    Map(
      SurveyDM.TEMPLATE_ID -> templateId.getOrElse(""),
      SurveyDM.CHANNEL -> channel.getOrElse(""),
      SurveyDM.ANSWERS -> answers
        .filterNot(_.isNull)
        .filterNot(_.isMissingNode)
        .map(_.toString)
        .getOrElse("{}"),
      SurveyDM.SEARCH_METADATA -> searchMetadata
        .filterNot(_.isNull)
        .filterNot(_.isMissingNode)
        .map(_.toString)
        .getOrElse("{}")
    )
  }
}
