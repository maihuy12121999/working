package rever.etl.emc.jobs.sys_contact

import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.Row
import rever.etl.emc.domain.SysContactFields
import rever.etl.rsparkflow.utils.{JsonUtils, Utils}

/** @author anhlt
  */
object SysContactSchema {

  def generateRow(row: Row): Seq[Row] = {

    val cartesianRows = Utils.cartesianProduct(
      List(
        List(row.getAs[Long](SysContactFields.DATE)),
        List(row.getAs[String](SysContactFields.BUSINESS_UNIT), "all"),
        List(row.getAs[String](SysContactFields.MARKET_CENTER_ID), "all"),
        List(row.getAs[String](SysContactFields.TEAM_ID), "all"),
        List(row.getAs[String](SysContactFields.USER_TYPE), "all"),
        List(row.getAs[String](SysContactFields.AGENT_ID), "all"),
        List(row.getAs[String](SysContactFields.STATUS), "all"),
        List(row.getAs[String](SysContactFields.CID))
      )
    )
    cartesianRows.flatMap(r => Seq(Row.fromSeq(r)))
  }

  def toNewSysContactRecord(row: Row): JsonNode = {
    val jsonRow = JsonUtils.toJson(
      Map(
        "data_type" -> row.getAs[String](SysContactFields.DATA_TYPE),
        "date" -> row.getAs[Long](SysContactFields.DATE),
        "business_unit" -> row.getAs[String](SysContactFields.BUSINESS_UNIT),
        "market_center_id" -> row.getAs[String](SysContactFields.MARKET_CENTER_ID),
        "team_id" -> row.getAs[String](SysContactFields.TEAM_ID),
        "user_type" -> row.getAs[String](SysContactFields.USER_TYPE),
        "agent_id" -> row.getAs[String](SysContactFields.AGENT_ID),
        "status" -> row.getAs[String](SysContactFields.STATUS),
        "num_contacts" -> row.getAs[Long](SysContactFields.NUM_CONTACT)
      )
    )
    JsonUtils.toJsonNode(jsonRow)
  }

  def toTotalSysContactRecord(row: Row): JsonNode = {
    val jsonRow = JsonUtils.toJson(
      Map(
        "data_type" -> row.getAs[String](SysContactFields.DATA_TYPE),
        "date" -> row.getAs[Long](SysContactFields.DATE),
        "business_unit" -> row.getAs[String](SysContactFields.BUSINESS_UNIT),
        "market_center_id" -> row.getAs[String](SysContactFields.MARKET_CENTER_ID),
        "team_id" -> row.getAs[String](SysContactFields.TEAM_ID),
        "user_type" -> row.getAs[String](SysContactFields.USER_TYPE),
        "agent_id" -> row.getAs[String](SysContactFields.AGENT_ID),
        "status" -> row.getAs[String](SysContactFields.STATUS),
        "num_contacts" -> row.getAs[Long](SysContactFields.NUM_CONTACT)
      )
    )
    JsonUtils.toJsonNode(jsonRow)
  }

}
