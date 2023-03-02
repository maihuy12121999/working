package rever.etl.listing.view_impressions.reader

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

import scala.concurrent.duration.DurationInt

class ViewListingEventReader extends SourceReader {
  override def read(tableName: String, config: Config): Dataset[Row] = {
    val driver = config.get("CH_DRIVER")
    val host = config.get("CH_HOST")
    val port = config.getInt("CH_PORT")
    val userName = config.get("CH_USER_NAME")
    val password = config.get("CH_PASSWORD")

    val df = SparkSession.active.read
      .format("jdbc")
      .option("url", s"jdbc:clickhouse://$host:$port/segment_tracking")
      .options(Map("delimiter" -> ",", "header" -> "true", "inferSchema" -> "true"))
      .option("user", userName)
      .option("password", password)
      .option("driver", driver)
      .option("query", buildQuery(config))
      .load()
    df
  }

  private def buildQuery(config: Config): String = {
    val reportTime = config.getDailyReportTime
    s"""
       |SELECT toInt64(received_at) as received_at,
       |       IF(JSONHas(properties,'property_info'),JSONExtractString(properties,'property_info','id'),'') as property_id,
       |       IF(JSONHas(context,'page','url'),JSONExtractString(context,'page','url'),'') as url,
       |       anonymous_id,
       |       user_id
       |FROM raw_data_normalized
       |WHERE 1=1
       |  AND type = 'page' 
       |  AND property_id <> '' 
       |  AND received_at >= $reportTime AND received_at < ${reportTime + 1.days.toMillis}
       |""".stripMargin
  }
}
