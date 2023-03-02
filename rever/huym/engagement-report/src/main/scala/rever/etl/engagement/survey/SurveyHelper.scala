package rever.etl.engagement.survey

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.Row
import rever.etl.engagement.domain.SurveyFields
import rever.etl.rsparkflow.utils.{JsonUtils, Utils}

object SurveyHelper {

  def generateRow(row: Row): Seq[Row] = {
    val cartesianRows = Utils.cartesianProduct(
      List(
        List(row.getAs[Long](SurveyFields.DATE)),
        List(row.getAs[String](SurveyFields.BUSINESS_UNIT), "all"),
        List(row.getAs[String](SurveyFields.MARKET_CENTER_ID), "all"),
        List(row.getAs[String](SurveyFields.TEAM_ID), "all"),
        List(row.getAs[String](SurveyFields.USER_TYPE)),
        List(row.getAs[String](SurveyFields.AGENT_ID)),
        List(row.getAs[String](SurveyFields.SOURCE)),
        List(row.getAs[String](SurveyFields.CID), "all"),
        List(row.getAs[String](SurveyFields.TEMPLATE_ID)),
        List(row.getAs[String](SurveyFields.CHANNEL)),
        List(row.getAs[String](SurveyFields.SURVEY_STATUS)),
        List(row.getAs[String](SurveyFields.SURVEY_ID))
      )
    )
    cartesianRows.flatMap(r => Seq(Row.fromSeq(r)))
  }

  def toNewSurveyRecord(row: Row): JsonNode = {
    val jsonRow = JsonUtils.toJson(
      Map(
        "data_type" -> row.getAs[String](SurveyFields.DATA_TYPE),
        "date" -> row.getAs[Long](SurveyFields.DATE),
        "business_unit" -> row.getAs[String](SurveyFields.BUSINESS_UNIT),
        "market_center_id" -> row.getAs[String](SurveyFields.MARKET_CENTER_ID),
        "team_id" -> row.getAs[String](SurveyFields.TEAM_ID),
        "user_type" -> row.getAs[String](SurveyFields.USER_TYPE),
        "agent_id" -> row.getAs[String](SurveyFields.AGENT_ID),
        "source" -> row.getAs[String](SurveyFields.SOURCE),
        "cid" -> row.getAs[String](SurveyFields.CID),
        "template_id" -> row.getAs[String](SurveyFields.TEMPLATE_ID),
        "channel" -> row.getAs[String](SurveyFields.CHANNEL),
        "survey_status" -> row.getAs[String](SurveyFields.SURVEY_STATUS),
        "total_survey" -> row.getAs[Long](SurveyFields.TOTAL_SURVEY)
      )
    )
    JsonUtils.toJsonNode(jsonRow)
  }
}
