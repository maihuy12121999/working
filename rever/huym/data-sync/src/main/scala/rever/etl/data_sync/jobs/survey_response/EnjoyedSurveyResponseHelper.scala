package rever.etl.data_sync.jobs.survey_response

import org.apache.avro.data.Json
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{ArrayType, IntegerType, LongType, StringType, StructField, StructType}
import rever.etl.data_sync.util.JsonUtils
import rever.etl.rsparkflow.client.DataMappingClient

import java.util.regex.Pattern
import scala.collection.mutable

object EnjoyedSurveyResponseHelper {
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

  final val WON = "won"
  final val HOT_PROSPECT = "hot_prospect"
  final val LOST = "lost"
  final val CLOSED = "closed"
  final val TRANSACTION = "transaction"
  final val ACTIVE = "active"

  final val STAGE_WON = "won"
  final val STAGE_HOT_PROSPECT = "hot_prospect"
  final val STAGE_LOST = "lost"
  final val STAGE_CLOSED = "closed"
  final val STAGE_UNDER_CONTRACT = "under_contract"
  final val STAGE_DECISION = "decision"
  final val STAGE_APPOINTMENT = "appointment"

  final val mPhase: Map[String, String] = Map(
    "won"->WON,
    "lost" -> LOST,
    "closed" -> CLOSED,
    "hotprospect" -> HOT_PROSPECT,
    "transaction" -> TRANSACTION,
    "active" -> ACTIVE
  )

  final val mStage: Map[String,String] = Map(
    WON -> STAGE_WON,
    LOST -> STAGE_LOST,
    HOT_PROSPECT -> STAGE_HOT_PROSPECT,
    TRANSACTION -> STAGE_UNDER_CONTRACT
  )
  final val marketCenterPattern = Pattern.compile("^(?<mc>((PDN)(\\d+)?)|([PA]\\d+))(-\\d+)?$")
  final val marketCenterTemplate= "^(((PDN)(\\d+)?)|([PA]\\d+))$"
  final val teamNameTemplate = "^(((PDN)(\\d+)?(-\\d+)?)|([PA]\\d+)(-\\d+)?)$"
  final val getFieldFromJsonUdf = udf[String, String, String](getFieldFromJsonString)

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

  def getFieldFromJsonString(fieldName: String, jsonString: String): String = {
    JsonUtils.toJsonNode(jsonString).at(fieldName).asText("")
  }


  def toSurveyResponseRecord(row: Row): Map[String, Any] = {
    Map[String, Any](
      EnjoyedSurveyResponseHelper.SURVEY_SYSTEM -> row.getAs[String](SURVEY_SYSTEM),
      EnjoyedSurveyResponseHelper.SURVEY_ID -> row.getAs[String](EnjoyedSurveyResponseHelper.SURVEY_ID),
      EnjoyedSurveyResponseHelper.PUBLIC_SURVEY_ID -> row.getAs[String](EnjoyedSurveyResponseHelper.PUBLIC_SURVEY_ID),
      EnjoyedSurveyResponseHelper.PROJECT_ID -> row.getAs[String](EnjoyedSurveyResponseHelper.PROJECT_ID),
      EnjoyedSurveyResponseHelper.CHANNEL -> row.getAs[String](EnjoyedSurveyResponseHelper.CHANNEL),
      EnjoyedSurveyResponseHelper.SURVEY_TYPE -> row.getAs[String](EnjoyedSurveyResponseHelper.SURVEY_TYPE),
      EnjoyedSurveyResponseHelper.PHASE -> row.getAs[String](EnjoyedSurveyResponseHelper.PHASE),
      EnjoyedSurveyResponseHelper.STAGE -> row.getAs[String](EnjoyedSurveyResponseHelper.STAGE),
      EnjoyedSurveyResponseHelper.PIPELINE_ID -> row.getAs[String](EnjoyedSurveyResponseHelper.PIPELINE_ID),
      EnjoyedSurveyResponseHelper.BUSINESS_UNIT -> row.getAs[String](EnjoyedSurveyResponseHelper.BUSINESS_UNIT),
      EnjoyedSurveyResponseHelper.MARKET_CENTER_ID -> row.getAs[String](EnjoyedSurveyResponseHelper.MARKET_CENTER_ID),
      EnjoyedSurveyResponseHelper.TEAM_ID -> row.getAs[String](EnjoyedSurveyResponseHelper.TEAM_ID),
      EnjoyedSurveyResponseHelper.AGENT_ID -> row.getAs[String](EnjoyedSurveyResponseHelper.AGENT_ID),
      EnjoyedSurveyResponseHelper.CID -> row.getAs[String](EnjoyedSurveyResponseHelper.CID),
      EnjoyedSurveyResponseHelper.P_CID -> row.getAs[String](EnjoyedSurveyResponseHelper.P_CID),
      EnjoyedSurveyResponseHelper.SCORE -> row.getAs[Int](EnjoyedSurveyResponseHelper.SCORE),
      EnjoyedSurveyResponseHelper.COMMENT -> row.getAs[String](EnjoyedSurveyResponseHelper.COMMENT),
      EnjoyedSurveyResponseHelper.PROPERTIES -> row.getAs[String](EnjoyedSurveyResponseHelper.PROPERTIES),
      EnjoyedSurveyResponseHelper.NOTES -> row.getAs[mutable.WrappedArray[String]](EnjoyedSurveyResponseHelper.NOTES).toArray,
      EnjoyedSurveyResponseHelper.TAGS -> row.getAs[mutable.WrappedArray[String]](EnjoyedSurveyResponseHelper.TAGS).toArray,
      EnjoyedSurveyResponseHelper.SENT_INFO -> row.getAs[String](EnjoyedSurveyResponseHelper.SENT_INFO),
      EnjoyedSurveyResponseHelper.LAST_RESPONSE -> row.getAs[String](EnjoyedSurveyResponseHelper.LAST_RESPONSE),
      EnjoyedSurveyResponseHelper.RESPONSES -> row.getAs[mutable.WrappedArray[String]](EnjoyedSurveyResponseHelper.RESPONSES).toArray,
      EnjoyedSurveyResponseHelper.TOTAL_RESPONSE_SCORE -> row.getAs[Int](EnjoyedSurveyResponseHelper.TOTAL_RESPONSE_SCORE),
      EnjoyedSurveyResponseHelper.TOTAL_RESPONSE -> row.getAs[Int](EnjoyedSurveyResponseHelper.TOTAL_RESPONSE),
      EnjoyedSurveyResponseHelper.TOTAL_ACCESS -> row.getAs[Int](EnjoyedSurveyResponseHelper.TOTAL_ACCESS),
      EnjoyedSurveyResponseHelper.LAST_ACCESS_CLIENT_INFO -> row.getAs[String](EnjoyedSurveyResponseHelper.LAST_ACCESS_CLIENT_INFO),
      EnjoyedSurveyResponseHelper.LAST_ACCESS_CLIENT_INFO_PER_CHANNEL -> row.getAs[String](EnjoyedSurveyResponseHelper.LAST_ACCESS_CLIENT_INFO_PER_CHANNEL),
      EnjoyedSurveyResponseHelper.CREATED_TIME -> row.getAs[Long](EnjoyedSurveyResponseHelper.CREATED_TIME),
      EnjoyedSurveyResponseHelper.UPDATED_TIME -> row.getAs[Long](EnjoyedSurveyResponseHelper.UPDATED_TIME),
      EnjoyedSurveyResponseHelper.SENT_TIME -> row.getAs[Long](EnjoyedSurveyResponseHelper.SENT_TIME),
      EnjoyedSurveyResponseHelper.FIRST_SUBMIT_TIME -> row.getAs[Long](EnjoyedSurveyResponseHelper.FIRST_SUBMIT_TIME),
      EnjoyedSurveyResponseHelper.LAST_SUBMIT_TIME -> row.getAs[Long](EnjoyedSurveyResponseHelper.LAST_SUBMIT_TIME),
      EnjoyedSurveyResponseHelper.LOG_TIME -> row.getAs[Long](EnjoyedSurveyResponseHelper.LOG_TIME)
    )
  }

}
case class Mapping(
    phaseStandardizedMapping: Map[String,String],
    phaseToStageMapping: Map[String,String],
    oppoStageToPhaseIdMapping: Map[String,Seq[String]],
    businessMapping: Map[String,String]
                  )
