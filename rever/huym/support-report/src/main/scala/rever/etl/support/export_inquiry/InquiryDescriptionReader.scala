package rever.etl.support.export_inquiry

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.rsparkflow.spark.api.SourceReader
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.utils.TimestampUtils

class InquiryDescriptionReader extends SourceReader {

    override def read(s: String, config: Config): Dataset[Row] = {
      val driver = config.get("CH_DRIVER")
      val host = config.get("CH_HOST")
      val port = config.getInt("CH_PORT")
      val userName = config.get("CH_USER_NAME")
      val password = config.get("CH_PASSWORD")
      val db = config.get("CH_DB")

      val df = SparkSession.active.read
        .format("jdbc")
        .option("url", s"jdbc:clickhouse://$host:$port/$db")
        .options(Map("delimiter" -> ",", "header" -> "true", "inferSchema" -> "true"))
        .option("user", userName)
        .option("password", password)
        .option("driver", driver)
        .option("query", buildQuery(config))
        .load()
      df

  }
  private def buildQuery(config: Config): String = {

    val fromTime = TimestampUtils.parseMillsFromString(config.get("forcerun.from_date"),"yyyy-MM-dd")
    val toTime = TimestampUtils.parseMillsFromString(config.get("forcerun.to_date"),"yyyy-MM-dd")

    val sourceTable = config.get("source_table")

    s"""
       |SELECT
       |    distinct description
       |FROM $sourceTable
       |WHERE 1=1
       |    AND updated_time >= $fromTime
       |    AND updated_time <= $toTime
       |""".stripMargin
  }

}
