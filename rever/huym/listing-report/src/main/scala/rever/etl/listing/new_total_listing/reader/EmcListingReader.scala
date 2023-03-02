package rever.etl.listing.new_total_listing.reader

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.listing.new_total_listing.EmcListingHelper
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

import scala.concurrent.duration.DurationInt

class EmcListingReader extends SourceReader{

  override def read(s: String, config: Config): Dataset[Row] = {
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

    df
  }

  private def buildQuery(config: Config): String = {

    val reportTime = config.getDailyReportTime

    s"""
       |SELECT
       |    toInt64(timestamp) AS ${EmcListingHelper.TIMESTAMP},
       |    'REVER' AS ${EmcListingHelper.LISTING_CHANNEL},
       |    ${EmcListingHelper.MARKET_CENTER_ID},
       |    dictGetOrDefault('default.dict_team', 'name', tuple(assumeNotNull(${EmcListingHelper.MARKET_CENTER_ID})), '') AS ${EmcListingHelper.MARKET_CENTER_NAME},
       |    ${EmcListingHelper.TEAM_ID},
       |    dictGetOrDefault('default.dict_team', 'name', tuple(assumeNotNull(${EmcListingHelper.TEAM_ID})), '') AS ${EmcListingHelper.TEAM_NAME},
       |    IF (JSONHas(new_data, 'address', 'city_id'), JSONExtractString(new_data, 'address', 'city_id'), JSONExtractString(old_data, 'address', 'city_id')) AS ${EmcListingHelper.CITY_ID},
       |    IF (JSONHas(new_data, 'address', 'district_id'), JSONExtractString(new_data, 'address', 'district_id'), JSONExtractString(old_data, 'address', 'district_id')) AS ${EmcListingHelper.DISTRICT_ID},
       |    IF (JSONHas(new_data, 'project_id'), JSONExtractString(new_data, 'project_id'), JSONExtractString(old_data, 'project_id')) AS ${EmcListingHelper.PROJECT_ID},
       |    IF (JSONHas(new_data, 'owner_id'), JSONExtractString(new_data, 'owner_id'), JSONExtractString(old_data, 'owner_id')) AS ${EmcListingHelper.AGENT_ID},
       |    IF (JSONHas(performer, 'id'), JSONExtractString(performer, 'id'), '') AS ${EmcListingHelper.PUBLISHED_ID},
       |    toString(IF (JSONHas(new_data, 'service_type'), JSONExtract(new_data, 'service_type', 'Int8'), JSONExtract(old_data, 'service_type', 'Int8'))) AS ${EmcListingHelper.SERVICE_TYPE},
       |    toString(IF (JSONHas(new_data, 'property_status'), JSONExtract(new_data, 'property_status', 'Int16'), JSONExtract(old_data, 'property_status', 'Int16'))) AS ${EmcListingHelper.PROPERTY_STATUS},
       |    toString(IF (JSONHas(new_data, 'transaction', 'stage'), JSONExtract(new_data, 'transaction', 'stage', 'Int8'), JSONExtract(old_data, 'transaction', 'stage', 'Int8'))) AS ${EmcListingHelper.PROPERTY_TRANSACTION_STAGE},
       |    property_id as ${EmcListingHelper.LISTING_ID}
       |FROM listing_action
       |WHERE 1=1
       |    AND timestamp >= $reportTime
       |    AND timestamp < ${reportTime + 1.days.toMillis}
       |""".stripMargin
  }
}

