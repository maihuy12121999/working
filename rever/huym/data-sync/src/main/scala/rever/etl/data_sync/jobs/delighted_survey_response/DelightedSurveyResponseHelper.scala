package rever.etl.data_sync.jobs.delighted_survey_response

import org.apache.spark.sql.Row
import org.apache.spark.sql.types._
import rever.etl.data_sync.util.JsonUtils

import java.util.regex.Pattern
import scala.collection.mutable

object DelightedSurveyResponseHelper {
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
  final val RVA_EMAIL = "rva_email"
  final val CONTACT = "contact"

  final val LOG_TIME = "log_time"

  final val surveyResponseSchema = StructType(
    Array(
      StructField(SURVEY_SYSTEM, StringType, false),
      StructField(SURVEY_ID, StringType, false),
      StructField(PUBLIC_SURVEY_ID, StringType, false),
      StructField(PROJECT_ID, StringType, false),
      StructField(CHANNEL, StringType, false),
      StructField(SURVEY_TYPE, StringType, false),
      StructField(PHASE, StringType, false),
      StructField(STAGE, StringType, false),
      StructField(PIPELINE_ID, StringType, false),
      StructField(BUSINESS_UNIT, StringType, false),
      StructField(MARKET_CENTER_ID, StringType, false),
      StructField(TEAM_ID, StringType, false),
      StructField(AGENT_ID, StringType, false),
      StructField(CID, StringType, false),
      StructField(P_CID, StringType, false),
      StructField(SCORE, IntegerType, false),
      StructField(COMMENT, StringType, false),
      StructField(PROPERTIES, StringType, false),
      StructField(NOTES, ArrayType.apply(StringType), false),
      StructField(TAGS, ArrayType.apply(StringType), false),
      StructField(SENT_INFO, StringType, false),
      StructField(LAST_RESPONSE, StringType, false),
      StructField(RESPONSES, ArrayType.apply(StringType), false),
      StructField(TOTAL_RESPONSE_SCORE, IntegerType, false),
      StructField(TOTAL_RESPONSE, IntegerType, false),
      StructField(TOTAL_ACCESS, IntegerType, false),
      StructField(LAST_ACCESS_CLIENT_INFO, StringType, false),
      StructField(LAST_ACCESS_CLIENT_INFO_PER_CHANNEL, StringType, false),
      StructField(CREATED_TIME, LongType, false),
      StructField(UPDATED_TIME, LongType, false),
      StructField(SENT_TIME, LongType, false),
      StructField(FIRST_SUBMIT_TIME, LongType, false),
      StructField(LAST_SUBMIT_TIME, LongType, false)
    )
  )

  final val marketCenterPattern = Pattern.compile("^(?<mc>((PDN)(\\d+)?)|([PA]\\d+))(-\\d+)?$")
  final val marketCenterTemplate = "^(((PDN)(\\d+)?)|([PA]\\d+))$"
  final val teamNameTemplate = "^(((PDN)(\\d+)?(-\\d+)?)|([PA]\\d+)(-\\d+)?)$"

  def getFieldFromJsonString(fieldName: String, jsonString: String): Option[String] = {
    Some(JsonUtils.toJsonNode(jsonString).at(fieldName).asText(""))
  }

  def toSurveyResponseRecord(row: Row): Map[String, Any] = {
    Map[String, Any](
      DelightedSurveyResponseHelper.SURVEY_SYSTEM -> row.getAs[String](SURVEY_SYSTEM),
      DelightedSurveyResponseHelper.SURVEY_ID -> row.getAs[String](DelightedSurveyResponseHelper.SURVEY_ID),
      DelightedSurveyResponseHelper.PUBLIC_SURVEY_ID -> row.getAs[String](DelightedSurveyResponseHelper.PUBLIC_SURVEY_ID),
      DelightedSurveyResponseHelper.PROJECT_ID -> row.getAs[String](DelightedSurveyResponseHelper.PROJECT_ID),
      DelightedSurveyResponseHelper.CHANNEL -> row.getAs[String](DelightedSurveyResponseHelper.CHANNEL),
      DelightedSurveyResponseHelper.SURVEY_TYPE -> row.getAs[String](DelightedSurveyResponseHelper.SURVEY_TYPE),
      DelightedSurveyResponseHelper.PHASE -> row.getAs[String](DelightedSurveyResponseHelper.PHASE),
      DelightedSurveyResponseHelper.STAGE -> row.getAs[String](DelightedSurveyResponseHelper.STAGE),
      DelightedSurveyResponseHelper.PIPELINE_ID -> row.getAs[String](DelightedSurveyResponseHelper.PIPELINE_ID),
      DelightedSurveyResponseHelper.BUSINESS_UNIT -> row.getAs[String](DelightedSurveyResponseHelper.BUSINESS_UNIT),
      DelightedSurveyResponseHelper.MARKET_CENTER_ID -> row.getAs[String](DelightedSurveyResponseHelper.MARKET_CENTER_ID),
      DelightedSurveyResponseHelper.TEAM_ID -> row.getAs[String](DelightedSurveyResponseHelper.TEAM_ID),
      DelightedSurveyResponseHelper.AGENT_ID -> row.getAs[String](DelightedSurveyResponseHelper.AGENT_ID),
      DelightedSurveyResponseHelper.CID -> row.getAs[String](DelightedSurveyResponseHelper.CID),
      DelightedSurveyResponseHelper.P_CID -> row.getAs[String](DelightedSurveyResponseHelper.P_CID),
      DelightedSurveyResponseHelper.SCORE -> row.getAs[Int](DelightedSurveyResponseHelper.SCORE),
      DelightedSurveyResponseHelper.COMMENT -> row.getAs[String](DelightedSurveyResponseHelper.COMMENT),
      DelightedSurveyResponseHelper.PROPERTIES -> row.getAs[String](DelightedSurveyResponseHelper.PROPERTIES),
      DelightedSurveyResponseHelper.NOTES -> row.getAs[mutable.WrappedArray[String]](DelightedSurveyResponseHelper.NOTES).toArray,
      DelightedSurveyResponseHelper.TAGS -> row.getAs[mutable.WrappedArray[String]](DelightedSurveyResponseHelper.TAGS).toArray,
      DelightedSurveyResponseHelper.SENT_INFO -> row.getAs[String](DelightedSurveyResponseHelper.SENT_INFO),
      DelightedSurveyResponseHelper.LAST_RESPONSE -> row.getAs[String](DelightedSurveyResponseHelper.LAST_RESPONSE),
      DelightedSurveyResponseHelper.RESPONSES -> row.getAs[mutable.WrappedArray[String]](DelightedSurveyResponseHelper.RESPONSES).toArray,
      DelightedSurveyResponseHelper.TOTAL_RESPONSE_SCORE -> row.getAs[Int](DelightedSurveyResponseHelper.TOTAL_RESPONSE_SCORE),
      DelightedSurveyResponseHelper.TOTAL_RESPONSE -> row.getAs[Int](DelightedSurveyResponseHelper.TOTAL_RESPONSE),
      DelightedSurveyResponseHelper.TOTAL_ACCESS -> row.getAs[Int](DelightedSurveyResponseHelper.TOTAL_ACCESS),
      DelightedSurveyResponseHelper.LAST_ACCESS_CLIENT_INFO -> row.getAs[String](DelightedSurveyResponseHelper.LAST_ACCESS_CLIENT_INFO),
      DelightedSurveyResponseHelper.LAST_ACCESS_CLIENT_INFO_PER_CHANNEL -> row.getAs[String](DelightedSurveyResponseHelper.LAST_ACCESS_CLIENT_INFO_PER_CHANNEL),
      DelightedSurveyResponseHelper.CREATED_TIME -> row.getAs[Long](DelightedSurveyResponseHelper.CREATED_TIME),
      DelightedSurveyResponseHelper.UPDATED_TIME -> row.getAs[Long](DelightedSurveyResponseHelper.UPDATED_TIME),
      DelightedSurveyResponseHelper.SENT_TIME -> row.getAs[Long](DelightedSurveyResponseHelper.SENT_TIME),
      DelightedSurveyResponseHelper.FIRST_SUBMIT_TIME -> row.getAs[Long](DelightedSurveyResponseHelper.FIRST_SUBMIT_TIME),
      DelightedSurveyResponseHelper.LAST_SUBMIT_TIME -> row.getAs[Long](DelightedSurveyResponseHelper.LAST_SUBMIT_TIME),
      DelightedSurveyResponseHelper.LOG_TIME -> row.getAs[Long](DelightedSurveyResponseHelper.LOG_TIME)
    )
  }

}
case class InteractionDate(
    day: String,
    month: String,
    year: String
)
case class Mapping(
    phaseStandardizedMapping: Map[String, String],
    phaseToStageMapping: Map[String, String]
)
