package rever.etl.engagement.linquiry.reader

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

import scala.concurrent.duration.DurationInt

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
  def initQuery(config: Config):String={
    val reportTime = config.getDailyReportTime
    s"""
       |SELECT
       |  listing_id,
       |  published_time,
       |  updated_time,
       |  listing_status,
       |  property_type,
       |  num_bed_room,
       |  area as area_using,
       |  city,
       |  district,
       |  ward,
       |  project_name,
       |  owner_id,
       |  sale_price
       |FROM (
       |    SELECT
       |        property_id as listing_id,
       |        toInt64(published_time) as published_time,
       |        toInt64(updated_time) as updated_time,
       |        toString(property_status) as listing_status,
       |        toString(property_type) as property_type,
       |        num_bed_room,
       |        JSONExtractFloat(additional_info, 'basic', 'sale_price') as sale_price,
       |        IF (JSONHas(additional_info, 'basic', 'area_using'), JSONExtractFloat(additional_info, 'basic', 'area_using'), IF (area_using != 0.0, area_using, 0.0)) as area_using,
       |        IF (area != 0.0, area, area_using) as area,
       |        JSONExtractString(address, 'city') as city,
       |        JSONExtractString(address,'district') as district,
       |        JSONExtractString(address,'ward') as ward,
       |        JSONExtractString(additional_info,'project_name') as project_name,
       |        owner_id
       |    FROM rever_search_property_1
       |    WHERE 1=1
       |)
       |""".stripMargin
  }
}
