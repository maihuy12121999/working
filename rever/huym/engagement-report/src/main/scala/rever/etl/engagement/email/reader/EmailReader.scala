package rever.etl.engagement.email.reader

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.engagement.email.EmailHelper
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

import scala.concurrent.duration.DurationInt

class EmailReader extends SourceReader {
  override def read(s: String, config: Config): Dataset[Row] = {
    val driver = config.get("CH_DRIVER")
    val host = config.get("CH_HOST")
    val port = config.getInt("CH_PORT")
    val userName = config.get("CH_USER_NAME")
    val password = config.get("CH_PASSWORD")

    val df = SparkSession.active.read
      .format("jdbc")
      .option("url", s"jdbc:clickhouse://$host:$port/historical")
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
       |    ${EmailHelper.TIMESTAMP},
       |    ${EmailHelper.TEAM_ID},
       |    ${EmailHelper.AGENT_ID},
       |    ${EmailHelper.CREATOR_ID},
       |    ${EmailHelper.CIDS},
       |    ${EmailHelper.DIRECTION},
       |    ${EmailHelper.EMAIL_STATUS},
       |    ${EmailHelper.EMAIL_ID}
       |FROM (
       |    SELECT
       |            IF (JSONHas(data, 'type'), JSONExtractString(data,'type'), JSONExtractString(old_data, 'type')) AS ${EmailHelper.TYPE},
       |            toInt64(timestamp) as timestamp,
       |            IF (JSONHas(data, 'owner_team'), JSONExtractString(data, 'owner_team'), JSONExtractString(old_data, 'owner_team')) as ${EmailHelper.TEAM_ID},
       |            IF (JSONHas(data, 'owner'), JSONExtractString(data, 'owner'), JSONExtractString(old_data, 'owner')) AS ${EmailHelper.AGENT_ID},
       |            IF (JSONHas(data, 'created_by'), JSONExtractString(data, 'created_by'), JSONExtractString(old_data, 'created_by')) AS  ${EmailHelper.CREATOR_ID},
       |            IF (JSONHas(data, 'associations', 'contacts'), JSONExtractRaw(COALESCE(data, '{}'), 'associations', 'contacts'), JSONExtractRaw(COALESCE(old_data, '{}'), 'associations', 'contacts')) AS  ${EmailHelper.CONTACTS},
       |            toString(arrayDistinct(arrayMap(i -> JSONExtractString(contacts, i + 1, 'cid'), range(JSONLength(contacts))))) AS  ${EmailHelper.CIDS},
       |            toString(IF (JSONHas(data, 'email_metadata', 'direction'), JSONExtract(data, 'email_metadata', 'direction', 'String'), JSONExtract(data, 'email_metadata', 'direction', 'String'))) as  ${EmailHelper.DIRECTION},
       |            toString(IF (JSONHas(data, 'email_metadata', 'send_status'), JSONExtract(data, 'email_metadata', 'send_status', 'Int8'), JSONExtract(data, 'email_metadata', 'send_status', 'Int8'))) as  ${EmailHelper.EMAIL_STATUS},
       |            toString(object_id) as  ${EmailHelper.EMAIL_ID}
       |    FROM engagement_historical_1
       |    WHERE 1=1
       |        AND  ${EmailHelper.TYPE} = 'email'
       |        AND timestamp >= $reportTime
       |        AND timestamp < ${reportTime + 1.days.toMillis}
       |)
       |""".stripMargin
  }
}

