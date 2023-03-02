package rever.etl.listing.reader

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

class ListingReader extends SourceReader{
  override def read(tableName: String, config: Config): Dataset[Row] = {
    val host = config.get("CH_HOST")
    val driver = config.get("CH_DRIVER")
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
  def initQuery(config: Config):String ={
    val reportTime = config.getDailyReportTime
    s"""
       |SELECT
       |    JSONExtractInt(transaction,'stage') as listing_status,
       |    JSONExtractInt(transaction,'updated_time') as updated_status_time,
       |    property_id as listing_id
       |FROM rever_search_property_1
       |""".stripMargin
  }
}
