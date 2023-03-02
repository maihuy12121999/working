package rever.etl.emc.jobs.personal_contact

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.Row
import rever.etl.emc.domain.PersonalContactFields
import rever.etl.rsparkflow.utils.{JsonUtils, Utils}

/** @author anhlt
  */
object PersonalContactSchema {

  def generateRow(row: Row): Seq[Row] = {

    val cartesianRows = Utils.cartesianProduct(
      List(
        List(row.getAs[Long](PersonalContactFields.DATE)),
        List(row.getAs[String](PersonalContactFields.BUSINESS_UNIT), "all"),
        List(row.getAs[String](PersonalContactFields.MARKET_CENTER_ID), "all"),
        List(row.getAs[String](PersonalContactFields.TEAM_ID), "all"),
        List(row.getAs[String](PersonalContactFields.USER_TYPE), "all"),
        List(row.getAs[String](PersonalContactFields.AGENT_ID), "all"),
        List(row.getAs[String](PersonalContactFields.STATUS), "all"),
        List(row.getAs[String](PersonalContactFields.P_CID))
      )
    )
    cartesianRows.flatMap(r => Seq(Row.fromSeq(r)))
  }

  def toNewPersonalContactRecord(row: Row): JsonNode = {
    val jsonRow = JsonUtils.toJson(
      Map(
        "data_type" -> row.getAs[String](PersonalContactFields.DATA_TYPE),
        "date" -> row.getAs[Long](PersonalContactFields.DATE),
        "business_unit" -> row.getAs[String](PersonalContactFields.BUSINESS_UNIT),
        "market_center_id" -> row.getAs[String](PersonalContactFields.MARKET_CENTER_ID),
        "team_id" -> row.getAs[String](PersonalContactFields.TEAM_ID),
        "user_type" -> row.getAs[String](PersonalContactFields.USER_TYPE),
        "agent_id" -> row.getAs[String](PersonalContactFields.AGENT_ID),
        "status" -> row.getAs[String](PersonalContactFields.STATUS),
        "num_contacts" -> row.getAs[Long](PersonalContactFields.NUM_CONTACT)
      )
    )
    JsonUtils.toJsonNode(jsonRow)
  }

  def toTotalPersonalContactRecord(row: Row): JsonNode = {
    val jsonRow = JsonUtils.toJson(
      Map(
        "data_type" -> row.getAs[String](PersonalContactFields.DATA_TYPE),
        "date" -> row.getAs[Long](PersonalContactFields.DATE),
        "business_unit" -> row.getAs[String](PersonalContactFields.BUSINESS_UNIT),
        "market_center_id" -> row.getAs[String](PersonalContactFields.MARKET_CENTER_ID),
        "team_id" -> row.getAs[String](PersonalContactFields.TEAM_ID),
        "user_type" -> row.getAs[String](PersonalContactFields.USER_TYPE),
        "agent_id" -> row.getAs[String](PersonalContactFields.AGENT_ID),
        "status" -> row.getAs[String](PersonalContactFields.STATUS),
        "num_contacts" -> row.getAs[Long](PersonalContactFields.NUM_CONTACT)
      )
    )
    JsonUtils.toJsonNode(jsonRow)
  }

}
