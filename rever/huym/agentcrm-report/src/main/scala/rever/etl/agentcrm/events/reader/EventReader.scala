package rever.etl.agentcrm.events.reader

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

import scala.concurrent.duration.DurationInt

class EventReader extends SourceReader {
  override def read(s: String, config: Config): Dataset[Row] = {
    val driver = config.get("CH_DRIVER")
    val host = config.get("CH_HOST")
    val port = config.get("CH_PORT")
    val userName = config.get("CH_USER_NAME")
    val passWord = config.get("CH_PASSWORD")
    SparkSession.active.read
      .format("jdbc")
      .option("url", s"jdbc:clickhouse://$host:$port/segment_tracking")
      .option("user", userName)
      .option("password", passWord)
      .option("driver", driver)
      .option("query", initQuery(config))
      .load()
  }

  private def initQuery(config: Config): String = {
    val reportTime = config.getHourlyReportTime
    s"""
       |SELECT
       |    user_id,
       |    JSONExtractString(context,'page','url') as url,
       |    JSONExtractString(context, 'userAgent') as user_agent,
       |    toInt64(toStartOfHour(toDateTime(received_at/1000,'Asia/Ho_Chi_Minh')))*1000 as date,
       |    extract(IF(JSONHas(properties, 'page_location'), JSONExtractString(properties, 'page_location'),JSONExtractString(context,'page','path')), '(.+?)(\\?.+)?$$') as page,
       |    event
       |FROM segment_tracking.raw_data_normalized
       |WHERE 1=1
       |    AND channel='client'
       |    AND type = 'track'
       |    AND received_at >=$reportTime
       |    AND received_at <(${reportTime + 1.hours.toMillis})
       |    AND (url like '%agentcrm-stag.rever.vn%' OR url like '%agentcrm-beta.rever.vn%' OR url like '%agentcrm.rever.vn%')
       |""".stripMargin
  }
}
