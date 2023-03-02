package rever.etl.emc.jobs.common

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.rsparkflow.api.configuration.Config

import scala.concurrent.duration.DurationInt

trait SysContactReaderMixin {

  protected def readSysContactDataset(config: Config): Dataset[Row] = {
    val driver = config.get("CH_DRIVER")
    val host = config.get("CH_HOST")
    val port = config.getInt("CH_PORT")
    val userName = config.get("CH_USER_NAME")
    val password = config.get("CH_PASSWORD")

    val fromTime = config.getDailyReportTime

    SparkSession.active.read
      .format("jdbc")
      .option("url", s"jdbc:clickhouse://$host:$port/airbyte")
      .option("inferSchema", "true")
      .option("user", userName)
      .option("password", password)
      .option("driver", driver)
      .option("query", buildQuery(config, fromTime, fromTime + 1.days.toMillis - 1))
      .load()

  }

  private def buildQuery(config: Config, fromTime: Long, toTime: Long): String = {
    config.isProductionMode match {
      case true =>
        s"""
           |SELECT
           | timestamp,
           | cid,
           | action,
           | creator,
           | updated_by,
           | properties,
           | status,
           | created_time,
           | updated_time,
           | source,
           | performer
           |FROM (
           |SELECT 
           |  timestamp,
           |  JSONExtractString(data, 'action') AS action,
           |  JSONExtractString(data, 'object_id') AS cid,
           |  JSONExtractRaw(data, 'snapshot_data') AS snapshot_data,
           |  JSONExtractString(snapshot_data, 'created_by') AS creator,
           |  JSONExtractString(snapshot_data, 'updated_by') AS updated_by,
           |  JSONExtractRaw(snapshot_data, 'properties') AS properties,
           |  toString(JSONExtractInt(snapshot_data, 'status')) AS status,
           |  JSONExtractInt(snapshot_data, 'created_time') AS created_time,
           |  JSONExtractInt(snapshot_data, 'updated_time') AS updated_time,
           |  JSONExtractString(data, 'source') AS source,
           |  JSONExtractString(data, 'performer') AS performer
           |FROM rap_unknown.kafka_record_unknown_v2
           |WHERE 1=1 
           |  AND topic='rap.historical.sys_contact_historical'
           |  AND `timestamp` >= $fromTime
           |  AND `timestamp` <= $toTime
           |)
           |""".stripMargin
      case false =>
        s"""
           |SELECT
           |  timestamp,
           |  action,
           |  object_id as cid,
           |  if( JSONHas(snapshot_data, 'cid'),JSONExtractString(snapshot_data,'created_by'), JSONExtractString(snapshot_old_data,'created_by')) as creator,
           |  if( JSONHas(snapshot_data, 'cid'),JSONExtractString(snapshot_data,'updated_by'), JSONExtractString(snapshot_old_data,'updated_by')) as updated_by,
           |  if( JSONHas(snapshot_data, 'cid'),JSONExtractRaw(snapshot_data,'properties'), JSONExtractRaw(snapshot_old_data,'properties')) as properties,
           |  toString(if(JSONHas(snapshot_data, 'cid'),JSONExtractInt(snapshot_data,'status'), JSONExtractInt(snapshot_old_data,'status'))) as status,
           |  source,
           |  performer,
           |  JSONExtractString(properties, 'phone') as phone,
           |  if( JSONHas(snapshot_data, 'cid'),JSONExtractInt(snapshot_data,'created_time'), JSONExtractInt(snapshot_old_data,'created_time')) as created_time,
           |  if( JSONHas(snapshot_data, 'cid'),JSONExtractInt(snapshot_data,'updated_time'), JSONExtractInt(snapshot_old_data,'updated_time')) as updated_time
           |FROM historical.sys_contact_historical_1
           |WHERE 1=1
           |  AND `timestamp` >= $fromTime
           |  AND `timestamp` <= $toTime
           |""".stripMargin
    }
  }

}
