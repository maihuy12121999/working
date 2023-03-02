package rever.etl.data_sync.domain.survey_response

import vn.rever.common.domain.Implicits.ImplicitAny
import vn.rever.jdbc.{JdbcRecord, SqlFieldMissing}

object SurveyResponseDM {
  final val SURVEY_SYSTEM = "survey_system"
  final val SURVEY_ID = "survey_id"
  final val PUBLIC_SURVEY_ID = "public_survey_id"
  final val PROJECT_ID = "project_id"
  final val CHANNEL = "channel"
  final val SURVEY_TYPE = "survey_type"
  final val PHASE = "phase"
  final val STAGE = "stage"
  final val PIPELINE_ID = "pipeline_id"
  final val BUSINESS_UNIT = "business_unit"
  final val MARKET_CENTER_ID = "market_center_id"
  final val TEAM_ID = "team_id"
  final val AGENT_ID = "agent_id"
  final val CID = "cid"
  final val P_CID = "p_cid"
  final val SCORE = "score"
  final val COMMENT = "comment"
  final val PROPERTIES = "properties"
  final val NOTES = "notes"
  final val TAGS = "tags"
  final val SENT_INFO = "sent_info"
  final val LAST_RESPONSE = "last_response"
  final val RESPONSES = "responses"
  final val TOTAL_RESPONSE_SCORE = "total_response_score"
  final val TOTAL_RESPONSE = "total_response"
  final val TOTAL_ACCESS = "total_access"
  final val LAST_ACCESS_CLIENT_INFO = "last_access_client_info"
  final val LAST_ACCESS_CLIENT_INFO_PER_CHANNEL = "last_access_client_info_per_channel"
  final val CREATED_TIME = "created_time"
  final val UPDATED_TIME = "updated_time"
  final val SENT_TIME = "sent_time"
  final val FIRST_SUBMIT_TIME = "first_submit_time"
  final val LAST_SUBMIT_TIME = "last_submit_time"
  final val LOG_TIME = "log_time"

  final val PRIMARY_IDS = Seq(SURVEY_ID, CREATED_TIME)
  final val FIELDS = Seq(
    SURVEY_SYSTEM,
    SURVEY_ID,
    PUBLIC_SURVEY_ID,
    PROJECT_ID,
    CHANNEL,
    SURVEY_TYPE,
    PHASE,
    STAGE,
    PIPELINE_ID,
    BUSINESS_UNIT,
    MARKET_CENTER_ID,
    TEAM_ID,
    AGENT_ID,
    CID,
    P_CID,
    SCORE,
    COMMENT,
    PROPERTIES,
    NOTES,
    TAGS,
    SENT_INFO,
    LAST_RESPONSE,
    RESPONSES,
    TOTAL_RESPONSE_SCORE,
    TOTAL_RESPONSE,
    TOTAL_ACCESS,
    LAST_ACCESS_CLIENT_INFO,
    LAST_ACCESS_CLIENT_INFO_PER_CHANNEL,
    CREATED_TIME,
    UPDATED_TIME,
    SENT_TIME,
    FIRST_SUBMIT_TIME,
    LAST_SUBMIT_TIME,
    LOG_TIME
  )
}
case class EnjoyedSurveyResponseRecord(
    var surveySystem: Option[String] = None,
    var surveyId: Option[String] = None,
    var publicSurveyId: Option[String] = None,
    var projectId: Option[String] = None,
    var channel: Option[String] = None,
    var surveyType: Option[String] = None,
    var phase: Option[String] = None,
    var stage: Option[String] = None,
    var pipelineId: Option[String] = None,
    var businessUnit: Option[String] = None,
    var marketCenterId: Option[String] = None,
    var teamId: Option[String] = None,
    var agentId: Option[String] = None,
    var cid: Option[String] = None,
    var pCid: Option[String] = None,
    var score: Option[Int] = None,
    var comment: Option[String] = None,
    var properties: Option[String] = None,
    var notes: Option[Array[String]] = None,
    var tags: Option[Array[String]] = None,
    var sentInfo: Option[String] = None,
    var lastResponse: Option[String] = None,
    var responses: Option[Array[String]] = None,
    var totalResponseScore: Option[Int] = None,
    var totalResponse: Option[Int] = None,
    var totalAccess: Option[Int] = None,
    var lastAccessClientInfo: Option[String] = None,
    var lastAccessClientInfoPerChannel: Option[String] = None,
    var createdTime: Option[Long] = None,
    var updatedTime: Option[Long] = None,
    var sentTime: Option[Long] = None,
    var firstSubmitTime: Option[Long] = None,
    var lastSubmitTime: Option[Long] = None,
    var logTime: Option[Long] = None
) extends JdbcRecord {
  override def getPrimaryKeys(): Seq[String] = SurveyResponseDM.PRIMARY_IDS

  override def getFields(): Seq[String] = SurveyResponseDM.FIELDS

  override def setValues(field: String, value: Any): Unit = field match {
    case SurveyResponseDM.SURVEY_SYSTEM                       => surveySystem = value.asOpt
    case SurveyResponseDM.SURVEY_ID                           => surveyId = value.asOpt
    case SurveyResponseDM.PUBLIC_SURVEY_ID                    => publicSurveyId = value.asOpt
    case SurveyResponseDM.PROJECT_ID                          => projectId = value.asOpt
    case SurveyResponseDM.CHANNEL                             => channel = value.asOpt
    case SurveyResponseDM.SURVEY_TYPE                         => surveyType = value.asOpt
    case SurveyResponseDM.PHASE                               => phase = value.asOpt
    case SurveyResponseDM.STAGE                               => stage = value.asOpt
    case SurveyResponseDM.PIPELINE_ID                         => pipelineId = value.asOpt
    case SurveyResponseDM.BUSINESS_UNIT                       => businessUnit = value.asOpt
    case SurveyResponseDM.MARKET_CENTER_ID                    => marketCenterId = value.asOpt
    case SurveyResponseDM.TEAM_ID                             => teamId = value.asOpt
    case SurveyResponseDM.AGENT_ID                            => agentId = value.asOpt
    case SurveyResponseDM.CID                                 => cid = value.asOpt
    case SurveyResponseDM.P_CID                               => pCid = value.asOpt
    case SurveyResponseDM.SCORE                               => score = value.asOpt
    case SurveyResponseDM.COMMENT                             => comment = value.asOpt
    case SurveyResponseDM.PROPERTIES                          => properties = value.asOpt
    case SurveyResponseDM.NOTES                               => notes = value.asOpt
    case SurveyResponseDM.TAGS                                => tags = value.asOpt
    case SurveyResponseDM.SENT_INFO                           => sentInfo = value.asOpt
    case SurveyResponseDM.LAST_RESPONSE                       => lastResponse = value.asOpt
    case SurveyResponseDM.RESPONSES                           => responses = value.asOpt
    case SurveyResponseDM.TOTAL_RESPONSE_SCORE                => totalResponseScore = value.asOpt
    case SurveyResponseDM.TOTAL_RESPONSE                      => totalResponse = value.asOpt
    case SurveyResponseDM.TOTAL_ACCESS                        => totalAccess = value.asOpt
    case SurveyResponseDM.LAST_ACCESS_CLIENT_INFO             => lastAccessClientInfo = value.asOpt
    case SurveyResponseDM.LAST_ACCESS_CLIENT_INFO_PER_CHANNEL => lastAccessClientInfoPerChannel = value.asOpt
    case SurveyResponseDM.CREATED_TIME                        => createdTime = value.asOpt
    case SurveyResponseDM.UPDATED_TIME                        => updatedTime = value.asOpt
    case SurveyResponseDM.SENT_TIME                           => sentTime = value.asOpt
    case SurveyResponseDM.FIRST_SUBMIT_TIME                   => firstSubmitTime = value.asOpt
    case SurveyResponseDM.LAST_SUBMIT_TIME                    => lastSubmitTime = value.asOpt
    case SurveyResponseDM.LOG_TIME                            => logTime = value.asOpt
    case _                                                    =>
  }

  override def getValue(field: String): Option[Any] = field match {
    case SurveyResponseDM.SURVEY_SYSTEM                       => surveySystem
    case SurveyResponseDM.SURVEY_ID                           => surveyId
    case SurveyResponseDM.PUBLIC_SURVEY_ID                    => publicSurveyId
    case SurveyResponseDM.PROJECT_ID                          => projectId
    case SurveyResponseDM.CHANNEL                             => channel
    case SurveyResponseDM.SURVEY_TYPE                         => surveyType
    case SurveyResponseDM.PHASE                               => phase
    case SurveyResponseDM.STAGE                               => stage
    case SurveyResponseDM.PIPELINE_ID                         => pipelineId
    case SurveyResponseDM.BUSINESS_UNIT                       => businessUnit
    case SurveyResponseDM.MARKET_CENTER_ID                    => marketCenterId
    case SurveyResponseDM.TEAM_ID                             => teamId
    case SurveyResponseDM.AGENT_ID                            => agentId
    case SurveyResponseDM.CID                                 => cid
    case SurveyResponseDM.P_CID                               => cid
    case SurveyResponseDM.SCORE                               => score
    case SurveyResponseDM.COMMENT                             => comment
    case SurveyResponseDM.PROPERTIES                          => properties
    case SurveyResponseDM.NOTES                               => notes
    case SurveyResponseDM.TAGS                                => tags
    case SurveyResponseDM.SENT_INFO                           => sentInfo
    case SurveyResponseDM.LAST_RESPONSE                       => lastResponse
    case SurveyResponseDM.RESPONSES                           => responses
    case SurveyResponseDM.TOTAL_RESPONSE_SCORE                => totalResponseScore
    case SurveyResponseDM.TOTAL_RESPONSE                      => totalResponse
    case SurveyResponseDM.TOTAL_ACCESS                        => totalAccess
    case SurveyResponseDM.LAST_ACCESS_CLIENT_INFO             => lastAccessClientInfo
    case SurveyResponseDM.LAST_ACCESS_CLIENT_INFO_PER_CHANNEL => lastAccessClientInfoPerChannel
    case SurveyResponseDM.CREATED_TIME                        => createdTime
    case SurveyResponseDM.UPDATED_TIME                        => updatedTime
    case SurveyResponseDM.SENT_TIME                           => sentTime
    case SurveyResponseDM.FIRST_SUBMIT_TIME                   => firstSubmitTime
    case SurveyResponseDM.LAST_SUBMIT_TIME                    => lastSubmitTime
    case SurveyResponseDM.LOG_TIME                            => logTime
    case _                                                    => throw SqlFieldMissing(field)
  }

  def toMap: Map[String, Any] = {
    SurveyResponseDM.FIELDS
      .map(field => field -> getValue(field).get)
      .toMap
  }
}
