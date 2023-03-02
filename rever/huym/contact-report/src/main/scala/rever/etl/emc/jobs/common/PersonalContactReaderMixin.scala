package rever.etl.emc.jobs.common

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.rsparkflow.api.configuration.Config

import scala.concurrent.duration.DurationInt

trait PersonalContactReaderMixin {

  protected def readDailyPContactDataset(config: Config): Dataset[Row] = {
    val driver = config.get("CH_DRIVER")
    val host = config.get("CH_HOST")
    val port = config.getInt("CH_PORT")
    val userName = config.get("CH_USER_NAME")
    val password = config.get("CH_PASSWORD")

    val reportTime = config.getDailyReportTime

    val df = SparkSession.active.read
      .format("jdbc")
      .option("url", s"jdbc:clickhouse://$host:$port/historical")
      .option("inferSchema", "true")
      .option("user", userName)
      .option("password", password)
      .option("driver", driver)
      .option("query", buildQuery(config, Seq.empty, reportTime, reportTime + 1.days.toMillis - 1))
      .load()

    println("Read P Contact dataset with schema:")
    df.printSchema()

    df
  }

  protected def readPContactDataset(
      config: Config,
      actions: Seq[String],
      fromTime: Long,
      toTime: Long
  ): Dataset[Row] = {

    val driver = config.get("CH_DRIVER")
    val host = config.get("CH_HOST")
    val port = config.getInt("CH_PORT")
    val userName = config.get("CH_USER_NAME")
    val password = config.get("CH_PASSWORD")

    val df = SparkSession.active.read
      .format("jdbc")
      .option("url", s"jdbc:clickhouse://$host:$port")
      .option("inferSchema", "true")
      .option("user", userName)
      .option("password", password)
      .option("driver", driver)
      .option("query", buildQuery(config, actions, fromTime, toTime))
      .load()

    println("Read P Contact dataset with schema:")
    df.printSchema()

    df
  }

  private def buildQuery(config: Config, actions: Seq[String], fromTime: Long, toTime: Long): String = {

    val actionCondition = if (Option(actions).forall(_.isEmpty)) {
      "1=1"
    } else {
      s"action IN (${actions.map(x => s"'$x'").mkString(",")})"
    }

    config.isProductionMode match {
      case true =>
        s"""
           |SELECT 
           | timestamp,
           | action,
           | p_cid,
           | cid,
           | owner,
           | phone,
           | user_tags,
           | system_tags,
           | properties,
           | JSONExtractString(properties, 'phone') AS phone,
           | JSONExtractRaw(properties, 'tags') AS user_tags,
           | JSONExtractRaw(properties, 'system_tags') AS system_tags,
           | status,
           | created_time,
           | updated_time,
           | source,
           | performer
           |FROM (
           |SELECT
           |  timestamp,
           |  JSONExtractString(data, 'object_id') AS p_cid,
           |  JSONExtractString(data, 'action') AS action,
           |  JSONExtractRaw(data, 'snapshot_data') AS snapshot_data,
           |  JSONExtractString(snapshot_data, 'cid') AS cid,
           |  JSONExtractString(snapshot_data, 'owner') AS owner,
           |  JSONExtractRaw(snapshot_data, 'properties') AS properties,
           |  toString(JSONExtractInt(snapshot_data, 'status')) AS status,
           |  JSONExtractInt(snapshot_data, 'created_time') AS created_time,
           |  JSONExtractInt(snapshot_data, 'updated_time') AS updated_time,
           |  JSONExtractString(data, 'source') AS source,
           |  JSONExtractString(data, 'performer') AS performer
           |FROM rap_unknown.kafka_record_unknown_v2
           |WHERE 1=1
           |  AND topic='rap.historical.personal_contact'
           |  AND $actionCondition
           |  AND `timestamp` >= $fromTime
           |  AND `timestamp` <= $toTime
           |)
           |""".stripMargin
      case false =>
        s"""
           |SELECT
           |  timestamp,
           |  action,
           |  object_id as p_cid,
           |  if(JSONHas(snapshot_data, 'p_cid'),JSONExtractString(snapshot_data,'cid'), JSONExtractString(snapshot_old_data,'cid')) as cid,
           |  if(JSONHas(snapshot_data, 'p_cid'),JSONExtractString(snapshot_data,'owner'), JSONExtractString(snapshot_old_data,'owner')) as owner,
           |  toString(if(JSONHas(snapshot_data, 'p_cid'),JSONExtractInt(snapshot_data,'status'), JSONExtractInt(snapshot_old_data,'status'))) as status,
           |  if(JSONHas(snapshot_data, 'p_cid'),JSONExtractRaw(snapshot_data,'properties'), JSONExtractRaw(snapshot_old_data,'properties')) as properties,
           |  source,
           |  performer,
           |  JSONExtractString(properties, 'phone') as phone,
           |  JSONExtractRaw(properties, 'tags') as user_tags,
           |  JSONExtractRaw(properties, 'system_tags') as system_tags,
           |  if(JSONHas(snapshot_data, 'p_cid'),JSONExtractInt(snapshot_data,'updated_time'), JSONExtractInt(snapshot_old_data,'updated_time')) as updated_time,
           |  if(JSONHas(snapshot_data, 'p_cid'),JSONExtractInt(snapshot_data,'created_time'), JSONExtractInt(snapshot_old_data,'created_time')) as created_time
           |from historical.personal_contact_1
           |where 1=1
           |  AND $actionCondition
           |  AND `timestamp` >= $fromTime
           |  AND `timestamp` <= $toTime
           |""".stripMargin
    }

  }

}
