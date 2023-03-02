package rever.etl.emc.enhancement.personal_contact.reader

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

import scala.concurrent.duration.DurationInt

class PContactListingReader extends SourceReader {
  override def read(tableName: String, config: Config): Dataset[Row] = {
    val driver = config.get("CH_DRIVER")
    val host = config.get("CH_HOST")
    val port = config.getInt("CH_PORT")
    val userName = config.get("CH_USER_NAME")
    val password = config.get("CH_PASSWORD")
    val reportTime = config.getDailyReportTime

    SparkSession.active.read
      .format("jdbc")
      .option("url", s"jdbc:clickhouse://$host:$port/datamart")
      .option("inferSchema", "true")
      .option("user", userName)
      .option("password", password)
      .option("driver", driver)
      .option("query", buildQuery(config, reportTime, reportTime + 1.days.toMillis - 1))
      .load()
  }

  private def buildQuery(config: Config, fromTime: Long, toTime: Long): String = {
    s"""
       |SELECT
       |    p_cid,
       |    0 as total_inquiries,
       |    count(distinct property_id) as total_listings
       |FROM(
       |  SELECT
       |    property_id,
       |    anyLast(p_cid) as p_cid,
       |    anyLast(cid) as cid,
       |    anyLast(property_status) as property_status
       |  FROM (
       |    SELECT
       |      property_id,
       |      JSONExtractString(contact, 'p_cid') as p_cid,
       |      JSONExtractString(contact, 'cid') as cid,
       |      property_status,
       |      updated_time
       |    FROM datamart.rever_search_property
       |    WHERE 1=1
       |    ORDER BY updated_time ASC
       |  )
       |  GROUP BY property_id
       |  HAVING property_status=700
       |)
       |GROUP BY p_cid
       |""".stripMargin
  }

}
