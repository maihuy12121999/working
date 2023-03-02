package rever.etl.listing.published_listings.reader

import org.apache.spark.sql.functions.{callUDF, col, lit}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.listing.domain.PublishedListingFields
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.api.udf.RUdfUtils

import scala.concurrent.duration.DurationInt

class ListingActionReader extends SourceReader {
  override def read(s: String, config: Config): Dataset[Row] = {
    val host = config.get("CH_HOST")
    val driver = config.get("CH_DRIVER")
    val port = config.get("CH_PORT")
    val userName = config.get("CH_USER_NAME")
    val passWord = config.get("CH_PASSWORD")

    val df = SparkSession.active.read
      .format("jdbc")
      .option("url", s"jdbc:clickhouse://$host:$port/log")
      .option("user", userName)
      .option("password", passWord)
      .option("driver", driver)
      .option("query", initQuery(config))
      .load()
      .withColumn(
        PublishedListingFields.BUSINESS_UNIT,
        callUDF(
          RUdfUtils.RV_DETECT_BUSINESS_UNIT,
          lit(""),
          col(PublishedListingFields.MARKET_CENTER_NAME),
          col(PublishedListingFields.TEAM_NAME)
        )
      )
      .withColumn(PublishedListingFields.TIMESTAMP, col(PublishedListingFields.TIMESTAMP).cast(LongType))
      .drop(PublishedListingFields.MARKET_CENTER_NAME, PublishedListingFields.TEAM_NAME)

    df
  }
  private def initQuery(config: Config): String = {
    val reportTime = config.getDailyReportTime
    s"""
       |SELECT
       |  timestamp,
       |  'REVER' as listing_channel,
       |  property_id,
       |  CASE JSONExtractInt(new_data,'service_type')
       |    WHEN 1 THEN 'rent'
       |    WHEN 2 THEN 'sale'
       |    WHEN 3 THEN 'sale_and_rent'
       |    ELSE 'unknown'
       |  END as service_type,
       |  market_center_id,
       |  team_id,
       |  dictGetOrDefault('default.dict_team', 'name', tuple(assumeNotNull(market_center_id)), '') AS market_center_name,
       |  dictGetOrDefault('default.dict_team', 'name', tuple(assumeNotNull(team_id)), '') AS team_name,
       |  JSONExtractString(new_data,'address','city_id') as city_id,
       |  JSONExtractString(new_data,'address','district_id') as district_id,
       |  project_id,
       |  owner_id as agent_id,
       |  JSONExtractString(performer,'id') as publisher_id,
       |  JSONExtractString(new_data,'contact','cid') as cid
       |FROM listing_action
       |WHERE 1=1
       |  AND JSONExtractInt(new_data,'transaction','stage') = 10
       |  AND timestamp >=$reportTime
       |  AND timestamp < ${reportTime + 1.days.toMillis}
       |""".stripMargin

  }
}
