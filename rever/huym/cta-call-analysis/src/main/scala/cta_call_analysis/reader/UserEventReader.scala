package cta_call_analysis.reader

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config
class UserEventReader extends SourceReader  {
  override def read(tableName: String, config: Config): Dataset[Row] = {
    val driver = config.get("CH_DRIVER")
    val host = config.get("CH_HOST")
    val port = config.get("CH_PORT")
    val userName = config.get("CH_USER_NAME")
    val passWord = config.get("CH_PASSWORD")
    SparkSession.active.read
      .format("jdbc")
      .option("url",s"jdbc:clickhouse://$host:$port/segment_tracking")
      .option("user",userName)
      .option("password",passWord)
      .option("driver",driver)
      .option("query",initQuery(config))
      .load()
  }

  def initQuery(config:Config):String={
    val reportTime = config.getDailyReportTime
    val ctaEvents = config.get("CTA_EVENTS")
    s"""
       |SELECT
       |    JSONExtract(context,'page','url','String') as url,
       |    timestamp,
       |    user_id
       |FROM raw_data_normalized
       |WHERE 1=1
       |    AND type = 'track'
       |    AND (url like '%rever.vn/mua/%' or url like '%rever.vn/du-an/%' or url like '%rever.vn/chuyen-vien/%')
       |    AND event in splitByString(',','$ctaEvents')
       |    AND timestamp >= $reportTime
       |    AND timestamp < ($reportTime+86400000)

       |""".stripMargin
  }
}

