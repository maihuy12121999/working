package cta_call_analysis.reader

import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

class   CallHistoryReader extends SourceReader {
  override def read(tableName: String, config: Config): Dataset[Row] = {
    val driver = config.get("CH_DRIVER")
    val host = config.get("CH_HOST")
    val port = config.get("CH_PORT")
    val userName = config.get("CH_USER_NAME")
    val passWord = config.get("CH_PASSWORD")
    SparkSession.active.read
      .format("jdbc")
      .option("url",s"jdbc:clickhouse://$host:$port/log")
      .option("user",userName)
      .option("password",passWord)
      .option("driver",driver)
      .option("query",initQuery(config))
      .load()

  }
  def initQuery(config:Config):String= {
    val reportTime = config.getDailyReportTime
    s"""
       |SELECT
       |    source as "from",
       |    destination as to,
       |    JSONExtract(sip,'id','String') as extension,
       |    status,
       |    duration,
       |    time as start_time,
       |    duration+time as end_time
       |FROM call_history_1
       |WHERE 1=1
       |  AND direction = 'inbound'
       |  AND start_time >= $reportTime
       |  AND start_time < $reportTime+(86400000)
       |""".stripMargin
  }
}
