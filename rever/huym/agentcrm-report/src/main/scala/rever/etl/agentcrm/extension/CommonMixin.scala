package rever.etl.agentcrm.extension

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{callUDF, col, expr, when}
import rever.etl.agentcrm.active_user.ActiveUserHelper
import rever.etl.agentcrm.config.FieldConfig
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.api.udf.RUdfUtils
import rever.etl.rsparkflow.client.DataMappingClient

import scala.collection.JavaConverters.asScalaBufferConverter

trait CommonMixin {

  protected def mGetUserTeams(userIds: Seq[String], config: Config): Map[String, String] = {
    val client = DataMappingClient.client(config)
    if (userIds.nonEmpty) {
      client.mGetUser(userIds).map(e => e._1 -> e._2.teamId(""))
    } else {
      Map.empty
    }
  }

  protected def toDepartment(userId: String, userTeamMap: Map[String, String], config: Config): String = {

    val techTeamIds = config.getList("tech_team_ids", ",").asScala.toSet
    userTeamMap.get(userId) match {
      case Some(teamId) =>
        if (techTeamIds.contains(teamId))
          "tech"
        else
          "non_tech"
      case None =>
        "unknown"
    }
  }

  def standardizeStringCols(df: DataFrame, cols: Seq[String], defaultValue: String): DataFrame = {

    cols.foldLeft(df)((df, colName) => {
      df.withColumn(
        colName,
        when(col(colName) === "" || col(colName).isNull, defaultValue).otherwise(col(colName))
      )
    })
  }

  def enhanceClientOSAndPlatform(activeUserRawDf: DataFrame, userAgentCol: String): DataFrame = {
    activeUserRawDf
      .withColumn("client_info", callUDF(RUdfUtils.RV_PARSE_USER_AGENT, col(userAgentCol)))
      .withColumn(FieldConfig.CLIENT_PLATFORM, col("client_info.platform"))
      .withColumn(FieldConfig.CLIENT_OS, col("client_info.os"))
      .drop("client_info")

  }
}
