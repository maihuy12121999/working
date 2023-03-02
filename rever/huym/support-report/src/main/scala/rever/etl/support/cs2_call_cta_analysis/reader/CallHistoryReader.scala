package rever.etl.support.cs2_call_cta_analysis.reader

import org.apache.spark.sql.functions.{col, lit}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.rsparkflow.spark.api.SourceReader
import rever.rsparkflow.spark.api.configuration.Config

import scala.concurrent.duration.DurationInt

class CallHistoryReader extends SourceReader {
  override def read(tableName: String, config: Config): Dataset[Row] = {
    val driver = config.get("CH_DRIVER")
    val host = config.get("CH_HOST")
    val port = config.getInt("CH_PORT")
    val userName = config.get("CH_USER_NAME")
    val password = config.get("CH_PASSWORD")

    val df = SparkSession.active.read
      .format("jdbc")
      .option("url", s"jdbc:clickhouse://$host:$port/log")
      .options(Map("delimiter" -> ",", "header" -> "true", "inferSchema" -> "true"))
      .option("user", userName)
      .option("password", password)
      .option("driver", driver)
      .option("query", buildQuery(config))
      .load()
      .withColumn("type", lit("call"))
      .orderBy(col("timestamp").asc)
    df
  }

  private def buildQuery(config: Config): String = {
    val reportTime = config.getDailyReportTime

    val query = s"""
       |SELECT
       |    call_id,
       |    call_direction,
       |    call_from,
       |    extension,
       |    sip_phone,
       |    call_to,
       |    call_status,
       |    dictGetOrDefault('default.dict_staff', 'full_name', tuple(assumeNotNull(owner)), '') AS agent,
       |    dictGetOrDefault('default.dict_staff', 'work_email', tuple(assumeNotNull(owner)), '') AS agent_email,
       |    dictGetOrDefault('default.dict_team', 'name', tuple(assumeNotNull(owner_team_id)), '') AS owner_team,
       |    timestamp,
       |    end_call_at,
       |    duration
       |FROM(
       |SELECT
       |    call_history_id as call_id,
       |    JSONExtractString(agent, 'username') AS owner,
       |    JSONExtractString(agent, 'team') AS owner_team_id,
       |    direction as call_direction,
       |    source AS call_from,
       |    destination AS call_to,
       |    JSONExtractString(sip,'id') as extension,
       |    JSONExtractString(sip,'phone') as sip_phone,
       |    status AS call_status,
       |    duration,
       |    time as timestamp,
       |    time+duration as end_call_at
       |FROM call_history_1
       |WHERE 
       |    1=1
       |    AND direction='inbound'
       |    AND time >= ${reportTime}
       |    AND time < ${reportTime + 1.days.toMillis}
       |    AND (group='' AND JSONExtractString(additional_info,'queue_hotline')='' AND JSONExtractString(additional_info,'ivr_hotline')='HOL')=0
       |)
       |""".stripMargin

    query
  }

}
