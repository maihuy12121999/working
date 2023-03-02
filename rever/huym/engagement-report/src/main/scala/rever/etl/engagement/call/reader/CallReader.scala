package rever.etl.engagement.call.reader

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.engagement.call.CallHelper
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

import scala.concurrent.duration.DurationInt

class CallReader extends SourceReader {
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
       |    ${CallHelper.TIMESTAMP},
       |    ${CallHelper.AGENT_ID},
       |    ${CallHelper.CREATOR_ID},
       |    ${CallHelper.SOURCE},
       |    ${CallHelper.CIDS},
       |    ${CallHelper.DIRECTION},
       |    ${CallHelper.CALL_STATUS},
       |    ${CallHelper.DURATION},
       |    ${CallHelper.CALL_ID}
       |FROM (
       |    SELECT
       |        IF (JSONHas(data, 'type'), JSONExtractString(data,'type'), JSONExtractString(old_data, 'type')) AS ${CallHelper.TYPE},
       |        toInt64(timestamp) as ${CallHelper.TIMESTAMP},
       |        IF (JSONHas(data, 'owner'), JSONExtractString(data, 'owner'), JSONExtractString(old_data, 'owner')) AS ${CallHelper.AGENT_ID},
       |        IF (JSONHas(data, 'created_by'), JSONExtractString(data, 'created_by'), JSONExtractString(old_data, 'created_by')) AS ${CallHelper.CREATOR_ID},
       |        IF (JSONHas(data, 'source'), JSONExtractString(data, 'source'), JSONExtractString(old_data, 'source')) as ${CallHelper.SOURCE},
       |        IF (JSONHas(data, 'associations', 'contacts'), JSONExtractRaw(COALESCE(data, '{}'), 'associations', 'contacts'), JSONExtractRaw(COALESCE(old_data, '{}'), 'associations', 'contacts')) AS ${CallHelper.CONTACTS},
       |        toString(arrayDistinct(arrayMap(i -> JSONExtractString(contacts, i + 1, 'cid'), range(JSONLength(contacts))))) AS ${CallHelper.CIDS},
       |        toString(IF (JSONHas(data, 'call_metadata', 'direction'), JSONExtract(data, 'call_metadata', 'direction', 'Int8'), JSONExtract(data, 'call_metadata', 'direction', 'Int8'))) as ${CallHelper.DIRECTION},
       |        toString(IF (JSONHas(data, 'call_metadata', 'status'), JSONExtract(data, 'call_metadata', 'status', 'Int8'), JSONExtract(data, 'call_metadata', 'status', 'Int8'))) as ${CallHelper.CALL_STATUS},
       |        IF (JSONHas(data, 'call_metadata', 'duration', 'Int64'), JSONExtract(data, 'call_metadata', 'duration', 'Int64'), JSONExtract(data, 'call_metadata', 'duration', 'Int64')) as ${CallHelper.DURATION},
       |        toString(object_id) as ${CallHelper.CALL_ID}
       |    FROM engagement_historical_1
       |    WHERE 1=1
       |        AND ${CallHelper.TYPE} = 'call'
       |        AND timestamp >= $reportTime
       |        AND timestamp < ${reportTime + 1.days.toMillis}
       |    )
       |""".stripMargin
  }
}
