package rever.etl.emc.enhancement.personal_contact.reader

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

import scala.concurrent.duration.DurationInt

class PContactInquiryReader extends SourceReader {
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
       |    count(distinct id) as total_inquiries,
       |    0 as total_listings
       |FROM(
       |    SELECT
       |    id,
       |    anyLast(p_cid) as p_cid,
       |    anyLast(cid) as cid,
       |    anyLast(status) as status
       |    FROM (
       |    SELECT
       |    id,
       |    p_cid,
       |    cid,
       |    status
       |    FROM datamart.inquiry
       |    WHERE 1=1
       |    ORDER BY updated_time ASC
       |    )
       |    GROUP BY id
       |    HAVING status=1
       |)
       |GROUP BY p_cid
       |""".stripMargin
  }

}
