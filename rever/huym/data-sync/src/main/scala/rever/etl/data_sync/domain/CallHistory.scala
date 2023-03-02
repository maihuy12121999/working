package rever.etl.data_sync.domain

import com.fasterxml.jackson.databind.JsonNode
import com.fasterxml.jackson.databind.node.TextNode
import vn.rever.common.util.JsonHelper

object CallHistoryDM {

  final val CALL_ID = "call_history_id"

  final val DIRECTION = "direction"
  final val SOURCE = "source"
  final val DESTINATION = "destination"
  final val NORMALIZED_SOURCE = "normalized_source"
  final val NORMALIZED_DESTINATION = "normalized_destination"
  final val CALL_SERVICE = "call_service"
  final val EXT_SERVICE_CALL_ID = "ext_service_call_id"
  final val INTERNAL_NUMBER = "internal_number"
  final val GROUP = "group"
  final val IVR = "ivr"
  final val QUEUE = "queue"
  final val TTA = "tta"
  final val DURATION = "duration"
  final val RECORDING_FILE = "recording_file"
  final val SIP = "sip"
  final val CID = "cid"
  final val P_CID = "p_cid"
  final val RAW_CONTACT_PHONE = "raw_contact_phone"
  final val CONTACT = "contact"
  final val MARKET_CENTER_ID = "market_center_id"
  final val TEAM_ID = "team_id"
  final val AGENT_ID = "agent_id"
  final val AGENT = "agent"
  final val ADDITIONAL_INFO = "additional_info"
  final val CLIENT_INFO = "client_info"
  final val TIME = "time"
  final val CREATED_TIME = "created_time"
  final val UPDATED_TIME = "updated_time"
  final val STATUS = "status"
  final val LOG_TIME = "log_time"

  final val PRIMARY_IDS = Seq(CALL_ID)

  val FIELDS = Seq(
    CALL_ID,
    DIRECTION,
    SOURCE,
    DESTINATION,
    NORMALIZED_SOURCE,
    NORMALIZED_DESTINATION,
    CALL_SERVICE,
    EXT_SERVICE_CALL_ID,
    INTERNAL_NUMBER,
    GROUP,
    IVR,
    QUEUE,
    TTA,
    DURATION,
    RECORDING_FILE,
    SIP,
    CID,
    P_CID,
    RAW_CONTACT_PHONE,
    CONTACT,
    MARKET_CENTER_ID,
    TEAM_ID,
    AGENT_ID,
    AGENT,
    ADDITIONAL_INFO,
    CLIENT_INFO,
    TIME,
    CREATED_TIME,
    UPDATED_TIME,
    STATUS
  )

}

object CallHistory {

  final val CALL_ID = "call_history_id"

  final val DIRECTION = "direction"
  final val SOURCE = "source"
  final val DESTINATION = "destination"
  final val CALL_SERVICE = "call_service"
  final val EXT_SERVICE_CALL_ID = "ext_service_call_id"
  final val INTERNAL_NUMBER = "internal_number"
  final val GROUP = "group"
  final val IVR = "ivr"
  final val QUEUE = "queue"
  final val TTA = "tta"
  final val DURATION = "duration"
  final val RECORDING_FILE = "recording_file"
  final val SIP = "sip"
  final val CID = "cid"
  final val RAW_CONTACT_PHONE = "raw_contact_phone"
  final val CONTACT = "contact"
  final val AGENT_ID = "agent_id"
  final val AGENT = "agent"
  final val ADDITIONAL_INFO = "additional_info"
  final val TIME = "time"
  final val CREATED_TIME = "created_time"
  final val UPDATED_TIME = "updated_time"
  final val STATUS = "status"

  final val PRIMARY_IDS = Seq(CALL_ID)

  val FIELDS = Seq(
    CALL_ID,
    DIRECTION,
    SOURCE,
    DESTINATION,
    CALL_SERVICE,
    EXT_SERVICE_CALL_ID,
    INTERNAL_NUMBER,
    GROUP,
    IVR,
    QUEUE,
    TTA,
    DURATION,
    RECORDING_FILE,
    SIP,
    CID,
    RAW_CONTACT_PHONE,
    CONTACT,
    AGENT_ID,
    AGENT,
    ADDITIONAL_INFO,
    TIME,
    CREATED_TIME,
    UPDATED_TIME,
    STATUS
  )

}

case class CallHistory(
    direction: Option[String],
    source: Option[String],
    destination: Option[String],
    callService: Option[String],
    extServiceCallId: Option[String],
    internalNumber: Option[String],
    group: Option[String],
    ivr: Option[String],
    queue: Option[String],
    tta: Option[Int],
    duration: Option[Int],
    recordingFile: Option[String],
    rawContactPhone: Option[String],
    sip: Option[JsonNode],
    contact: Option[JsonNode],
    agent: Option[JsonNode],
    additionalInfo: Option[JsonNode],
    time: Option[Long],
    createdTime: Option[Long],
    updatedTime: Option[Long],
    status: Option[String]
) {

  def cid: Option[String] = {
    contact
      .map(_.at("/id").asText(""))
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
  }

  def agentId: Option[String] = {
    agent
      .map(_.at("/username").asText(""))
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
  }

  def agentEmail: Option[String] = {
    agent
      .map(_.at("/email").asText(""))
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .filterNot(_.trim.isEmpty)
  }

  def agentMarketCenterId: Option[String] = {
    agent
      .map(_.at("/market_center").asText(""))
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .filterNot(_.trim.isEmpty)
  }

  def agentTeamId: Option[String] = {
    agent
      .map(_.at("/team").asText(""))
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
      .filterNot(_.trim.isEmpty)
  }

  def clientInfo: Option[JsonNode] = {
    additionalInfo
      .map(_.at("/client_custom_data"))
      .filterNot(_.isNull)
      .map {
        case node: TextNode =>
          val s = Option(node.asText("{}")).filterNot(_.isEmpty).getOrElse("{}")
          JsonHelper.fromJson[JsonNode](s)
        case node: JsonNode if node.isContainerNode => node
        case node                                   => JsonHelper.fromJson[JsonNode]("{}")
      }
  }
}
