package rever.etl.engagement.survey.reader

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.engagement.domain.SurveyFields
import rever.etl.engagement.survey.SurveyHelper
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

import scala.concurrent.duration.DurationInt

class SurveyReader extends SourceReader {
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
       |    ${SurveyFields.TIMESTAMP},
       |    ${SurveyFields.TEAM_ID},
       |    ${SurveyFields.AGENT_ID},
       |    ${SurveyFields.SOURCE},
       |    ${SurveyFields.CIDS},
       |    ${SurveyFields.TEMPLATE_ID},
       |    ${SurveyFields.CHANNEL},
       |    ${SurveyFields.SURVEY_STATUS},
       |    ${SurveyFields.SURVEY_ID}
       |FROM (
       |    SELECT
       |        IF (JSONHas(data, 'type'), JSONExtractString(data,'type'), JSONExtractString(old_data, 'type')) AS ${SurveyFields.TYPE},
       |        toInt64(timestamp) as ${SurveyFields.TIMESTAMP},
       |        IF (JSONHas(data, 'owner_team'), JSONExtractString(data, 'owner_team'), JSONExtractString(old_data, 'owner_team')) AS ${SurveyFields.TEAM_ID},
       |        IF (JSONHas(data, 'owner'), JSONExtractString(data, 'owner'), JSONExtractString(old_data, 'owner')) AS ${SurveyFields.AGENT_ID},
       |        IF (JSONHas(data, 'source'), JSONExtractString(data, 'source'), JSONExtractString(old_data, 'source')) as ${SurveyFields.SOURCE},
       |        IF (JSONHas(data, 'associations', 'contacts'), JSONExtractRaw(COALESCE(data, '{}'), 'associations', 'contacts'), JSONExtractRaw(COALESCE(old_data, '{}'), 'associations', 'contacts')) AS ${SurveyFields.CONTACTS},
       |        toString(arrayDistinct(arrayMap(i -> JSONExtractString(contacts, i + 1, 'cid'), range(JSONLength(contacts))))) AS ${SurveyFields.CIDS},
       |        toString(IF(JSONHas(data, 'survey_metadata', 'template_id'), JSONExtract(data, 'survey_metadata', 'template_id', 'Int16'), JSONExtract(old_data, 'survey_metadata', 'template_id', 'Int16'))) as ${SurveyFields.TEMPLATE_ID},
       |        IF(JSONHas(data, 'survey_metadata', 'channel'), JSONExtractString(data, 'survey_metadata', 'channel'), JSONExtractString(old_data, 'survey_metadata', 'channel')) as ${SurveyFields.CHANNEL},
       |        toString(IF (JSONHas(data,'status'), JSONExtract(data, 'status', 'Int8'), JSONExtract(old_data,  'status', 'Int8'))) as ${SurveyFields.SURVEY_STATUS},
       |        toString(object_id) as ${SurveyFields.SURVEY_ID}
       |    FROM engagement_historical_1
       |    WHERE 1=1
       |        AND ${SurveyFields.TYPE} = 'survey'
       |        AND ${SurveyFields.TIMESTAMP} >= $reportTime
       |        AND ${SurveyFields.TIMESTAMP} < ${reportTime + 1.days.toMillis}
       |    )
       |""".stripMargin
  }
}
