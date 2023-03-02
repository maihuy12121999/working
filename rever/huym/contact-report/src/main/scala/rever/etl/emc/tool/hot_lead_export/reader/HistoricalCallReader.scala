package rever.etl.emc.tool.hot_lead_export.reader

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.emc.tool.hot_lead_export.domain.HotLeadFields
import rever.etl.rsparkflow.utils.TimestampUtils

class HistoricalCallReader extends SourceReader {
  override def read(s: String, config: Config): Dataset[Row] = {
    val driver = config.get("CH_DRIVER")
    val host = config.get("CH_HOST")
    val port = config.getInt("CH_PORT")
    val userName = config.get("CH_USER_NAME")
    val password = config.get("CH_PASSWORD")
    val (fromTime, toTime) = (
      TimestampUtils.parseMillsFromString(config.get("forcerun.from_date"), "yyyy-MM-dd"),
      TimestampUtils.parseMillsFromString(config.get("forcerun.to_date"), "yyyy-MM-dd")
    )

    SparkSession.active.read
      .format("jdbc")
      .option("url", s"jdbc:clickhouse://$host:$port/datamart")
      .option("inferSchema", "true")
      .option("user", userName)
      .option("password", password)
      .option("driver", driver)
      .option("query", buildQuery(config, fromTime))
      .load()
      .withColumn(HotLeadFields.CALL_TIME, col(HotLeadFields.CALL_TIME).cast(LongType))
      .withColumn(HotLeadFields.DURATION, col(HotLeadFields.DURATION).cast(LongType))
  }

  private def buildQuery(config: Config, fromTime: Long): String = {
    s"""
       |select
       |    agent_id,
       |    destination,
       |    duration,
       |    status as call_status,
       |    toInt64(time) as call_time
       |from call_history
       |where 1=1
       |   and direction = 'outbound'
       |   and call_time >=$reportTime
       |""".stripMargin
  }
}
