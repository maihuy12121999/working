package rever.etl.data_sync.jobs.delighted_survey_response.reader

import org.apache.spark.sql.{DataFrame, SparkSession}
import rever.etl.data_sync.jobs.delighted_survey_response.DelightedSurveyResponseHelper
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

import scala.concurrent.duration.DurationInt

class DelightedSurveyResponseReader extends SourceReader {
  override def read(s: String, config: Config): DataFrame = {
    val driver = config.get("CH_DRIVER")
    val host = config.get("CH_HOST")
    val port = config.getInt("CH_PORT")
    val userName = config.get("CH_USER_NAME")
    val password = config.get("CH_PASSWORD")
    val database = config.get("source_db")

    val df = SparkSession.active.read
      .format("jdbc")
      .option("url", s"jdbc:clickhouse://$host:$port/$database")
      .options(Map("delimiter" -> ",", "header" -> "true", "inferSchema" -> "true"))
      .option("user", userName)
      .option("password", password)
      .option("driver", driver)
      .option("query", buildQuery(config))
      .load()

    df
  }

  def buildQuery(config: Config): String = {
    val sourceTable = config.get("source_table")

    val isSyncAll = config.getBoolean("is_sync_all", false)
    val (fromTime, toTime) = if (isSyncAll) {
      (0L, System.currentTimeMillis())
    } else {
      config.getExecutionDateInfo.getSyncDataTimeRange
    }

    s"""
       |SELECT
       |    'delighted' as ${DelightedSurveyResponseHelper.SURVEY_SYSTEM},
       |    toString(object_id) as ${DelightedSurveyResponseHelper.SURVEY_ID},
       |    '' as ${DelightedSurveyResponseHelper.PUBLIC_SURVEY_ID},
       |    toString(delighted_project_id) as ${DelightedSurveyResponseHelper.PROJECT_ID},
       |    IF(JSONHas(event_data, 'person_properties','channel'), JSONExtractString(event_data, 'person_properties','channel'),'') as ${DelightedSurveyResponseHelper.CHANNEL},
       |    toString(survey_type) as ${DelightedSurveyResponseHelper.SURVEY_TYPE},
       |    toString(IF(JSONHas(event_data, 'person_properties', 'hanh_dong'), JSONExtractString(event_data, 'person_properties', 'hanh_dong'),'')) as ${DelightedSurveyResponseHelper.PHASE},
       |    toString(business_unit) as ${DelightedSurveyResponseHelper.BUSINESS_UNIT},
       |    toString(owner_mc) as ${DelightedSurveyResponseHelper.MARKET_CENTER_ID},
       |    toString(owner_team) as ${DelightedSurveyResponseHelper.TEAM_ID},
       |    toString(owner) as ${DelightedSurveyResponseHelper.AGENT_ID},
       |    '' as ${DelightedSurveyResponseHelper.CID},
       |    '' as ${DelightedSurveyResponseHelper.P_CID},
       |    toInt16(score) as ${DelightedSurveyResponseHelper.SCORE},
       |    IF(JSONHas(event_data, 'comment'), JSONExtractString(event_data, 'comment'),'') as ${DelightedSurveyResponseHelper.COMMENT},
       |    IF(JSONHas(event_data, 'person_properties'), JSONExtractRaw(event_data, 'person_properties'),'') as ${DelightedSurveyResponseHelper.PROPERTIES},
       |    IF(JSONHas(event_data, 'notes'), JSONExtractRaw(event_data, 'notes'),'[]') as ${DelightedSurveyResponseHelper.NOTES},
       |    IF(JSONHas(event_data, 'tags'), JSONExtractRaw(event_data, 'tags'),'[]') as ${DelightedSurveyResponseHelper.TAGS},
       |    IF(JSONHas(event_data, 'person'), JSONExtractRaw(event_data, 'person'),'{}') as ${DelightedSurveyResponseHelper.SENT_INFO},
       |    coalesce(event_data, '{}') as response,
       |    '{}' as ${DelightedSurveyResponseHelper.LAST_ACCESS_CLIENT_INFO},
       |    '{}' as ${DelightedSurveyResponseHelper.LAST_ACCESS_CLIENT_INFO_PER_CHANNEL},
       |    IF(JSONHas(event_data, 'created_at'), JSONExtract(event_data, 'created_at','Int64'),0)*1000 as ${DelightedSurveyResponseHelper.CREATED_TIME},
       |    toInt64(timestamp) as ${DelightedSurveyResponseHelper.UPDATED_TIME},
       |    IF(JSONHas(event_data, 'send_survey_time'), JSONExtract(event_data, 'send_survey_time','Int64'),0) as ${DelightedSurveyResponseHelper.SENT_TIME},
       |    IF(JSONHas(event_data, 'updated_at'), JSONExtract(event_data, 'updated_at','Int64'),0)*1000 as submit_time
       |FROM $sourceTable
       |WHERE 1=1
       |""".stripMargin
  }
}
