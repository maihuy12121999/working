package rever.etl.data_sync.jobs.survey_response.reader

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import org.apache.spark.sql.{DataFrame, SparkSession}
import rever.etl.data_sync.jobs.survey_response.EnjoyedSurveyResponseHelper
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

class EnjoyedSurveyResponseReader extends SourceReader {
  override def read(s: String, config: Config): DataFrame = {
    val driver = config.get("MYSQL_DRIVER")
    val host = config.get("MYSQL_HOST")
    val port = config.getInt("MYSQL_PORT")
    val userName = config.get("MYSQL_USER_NAME")
    val password = config.get("MYSQL_PASSWORD")
    val database = config.get("MYSQL_DB")

    val df = SparkSession.active.read
      .format("jdbc")
      .option("url", s"jdbc:mysql://$host:$port/$database")
      .options(Map("delimiter" -> ",", "header" -> "true", "inferSchema" -> "true"))
      .option("user", userName)
      .option("password", password)
      .option("driver", driver)
      .option("query", buildQuery(config))
      .load()

    val finalDf = df.select(
      col(EnjoyedSurveyResponseHelper.SURVEY_SYSTEM).cast(StringType),
      col(EnjoyedSurveyResponseHelper.SURVEY_ID).cast(StringType),
      col(EnjoyedSurveyResponseHelper.PUBLIC_SURVEY_ID).cast(StringType),
      col(EnjoyedSurveyResponseHelper.PROJECT_ID).cast(StringType),
      col(EnjoyedSurveyResponseHelper.CHANNEL).cast(StringType),
      col(EnjoyedSurveyResponseHelper.SURVEY_TYPE).cast(StringType),
      col(EnjoyedSurveyResponseHelper.PHASE).cast(StringType),
      col(EnjoyedSurveyResponseHelper.PIPELINE_ID).cast(StringType),
      col(EnjoyedSurveyResponseHelper.BUSINESS_UNIT).cast(StringType),
      col(EnjoyedSurveyResponseHelper.MARKET_CENTER_ID).cast(StringType),
      col(EnjoyedSurveyResponseHelper.TEAM_ID).cast(StringType),
      col(EnjoyedSurveyResponseHelper.CID).cast(StringType),
      col(EnjoyedSurveyResponseHelper.P_CID).cast(StringType),
      col(EnjoyedSurveyResponseHelper.SCORE).cast(IntegerType),
      col(EnjoyedSurveyResponseHelper.COMMENT).cast(StringType),
      col(EnjoyedSurveyResponseHelper.PROPERTIES).cast(StringType),
      col(EnjoyedSurveyResponseHelper.NOTES).cast(StringType),
      col(EnjoyedSurveyResponseHelper.TAGS).cast(StringType),
      col(EnjoyedSurveyResponseHelper.SENT_INFO).cast(StringType),
      col(EnjoyedSurveyResponseHelper.LAST_RESPONSE).cast(StringType),
      col(EnjoyedSurveyResponseHelper.RESPONSES).cast(StringType),
      col(EnjoyedSurveyResponseHelper.TOTAL_ACCESS).cast(IntegerType),
      col("arr_score").cast(StringType),
      col(EnjoyedSurveyResponseHelper.TOTAL_RESPONSE).cast(IntegerType),
      col(EnjoyedSurveyResponseHelper.LAST_ACCESS_CLIENT_INFO).cast(StringType),
      col(EnjoyedSurveyResponseHelper.LAST_ACCESS_CLIENT_INFO_PER_CHANNEL).cast(StringType),
      col(EnjoyedSurveyResponseHelper.CREATED_TIME).cast(LongType),
      col(EnjoyedSurveyResponseHelper.UPDATED_TIME).cast(LongType),
      col(EnjoyedSurveyResponseHelper.SENT_TIME).cast(LongType),
      col("list_first_submit_time").cast(StringType),
      col(EnjoyedSurveyResponseHelper.LAST_SUBMIT_TIME).cast(LongType)
    )
    finalDf.printSchema()

    finalDf

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
       |    'enjoyed' as ${EnjoyedSurveyResponseHelper.SURVEY_SYSTEM},
       |    id as ${EnjoyedSurveyResponseHelper.SURVEY_ID},
       |    public_id as ${EnjoyedSurveyResponseHelper.PUBLIC_SURVEY_ID},
       |    project_id as ${EnjoyedSurveyResponseHelper.PROJECT_ID},
       |    IF(JSON_CONTAINS_PATH(last_response,'all','$$.channel'), JSON_UNQUOTE(JSON_EXTRACT(last_response,'$$.channel')),'') as ${EnjoyedSurveyResponseHelper.CHANNEL},
       |    case
       |      when survey_type = '1' then 'nps'
       |      when survey_type = '2' then 'csat'
       |      else ''
       |    end as ${EnjoyedSurveyResponseHelper.SURVEY_TYPE},
       |    IF(JSON_CONTAINS_PATH(properties,'all','$$.hanh_dong'), JSON_UNQUOTE(JSON_EXTRACT(properties,'$$.hanh_dong')),'') as ${EnjoyedSurveyResponseHelper.PHASE},
       |    IF(JSON_CONTAINS_PATH(properties,'all','$$.oppo_pipeline_id'), JSON_UNQUOTE(JSON_EXTRACT(properties,'$$.oppo_pipeline_id')),'') as ${EnjoyedSurveyResponseHelper.PIPELINE_ID},
       |    IF(JSON_CONTAINS_PATH(properties,'all','$$.business_unit'), JSON_UNQUOTE(JSON_EXTRACT(properties,'$$.business_unit')),'') as ${EnjoyedSurveyResponseHelper.BUSINESS_UNIT},
       |    IF(JSON_CONTAINS_PATH(properties,'all','$$.rva_mc_id'), JSON_UNQUOTE(JSON_EXTRACT(properties,'$$.rva_mc_id')),'') as ${EnjoyedSurveyResponseHelper.MARKET_CENTER_ID},
       |    IF(JSON_CONTAINS_PATH(properties,'all','$$.rva_team_id'), JSON_UNQUOTE(JSON_EXTRACT(properties,'$$.rva_team_id')),'') as ${EnjoyedSurveyResponseHelper.TEAM_ID},
       |    IF(JSON_CONTAINS_PATH(properties,'all','$$.cid'), JSON_UNQUOTE(JSON_EXTRACT(properties,'$$.cid')),'') as ${EnjoyedSurveyResponseHelper.CID},
       |    IF(JSON_CONTAINS_PATH(properties,'all','$$.p_cid'), JSON_UNQUOTE(JSON_EXTRACT(properties,'$$.p_cid')),'') as ${EnjoyedSurveyResponseHelper.P_CID},
       |    IF(JSON_CONTAINS_PATH(last_response,'all','$$.score'), JSON_UNQUOTE(JSON_EXTRACT(last_response,'$$.score')),'0') as ${EnjoyedSurveyResponseHelper.SCORE},
       |    IF(JSON_CONTAINS_PATH(last_response,'all','$$.comment'), JSON_UNQUOTE(JSON_EXTRACT(last_response,'$$.comment')),'') as ${EnjoyedSurveyResponseHelper.COMMENT},
       |    coalesce(properties,'{}') as ${EnjoyedSurveyResponseHelper.PROPERTIES},
       |    coalesce(notes,'[]') as ${EnjoyedSurveyResponseHelper.NOTES},
       |    coalesce(tags,'[]') as ${EnjoyedSurveyResponseHelper.TAGS},
       |    coalesce(send_info,'{}') as ${EnjoyedSurveyResponseHelper.SENT_INFO},
       |    coalesce(last_response,'{}') as ${EnjoyedSurveyResponseHelper.LAST_RESPONSE},
       |    coalesce(list_response,'[]') as ${EnjoyedSurveyResponseHelper.RESPONSES},
       |    num_access as ${EnjoyedSurveyResponseHelper.TOTAL_ACCESS},
       |    coalesce(JSON_UNQUOTE(JSON_EXTRACT(list_response,'$$**.score')),'[0]') as arr_score,
       |    coalesce(JSON_LENGTH(list_response),0) as ${EnjoyedSurveyResponseHelper.TOTAL_RESPONSE},
       |    coalesce(last_access_info,'{}') as ${EnjoyedSurveyResponseHelper.LAST_ACCESS_CLIENT_INFO},
       |    coalesce(last_access_info_per_channel,'{}') as ${EnjoyedSurveyResponseHelper.LAST_ACCESS_CLIENT_INFO_PER_CHANNEL},
       |    created_time as ${EnjoyedSurveyResponseHelper.CREATED_TIME},
       |    updated_time as ${EnjoyedSurveyResponseHelper.UPDATED_TIME},
       |    IF(JSON_CONTAINS_PATH(properties,'all','$$.send_survey_time'), JSON_UNQUOTE(JSON_EXTRACT(properties,'$$.send_survey_time')),'0') as ${EnjoyedSurveyResponseHelper.SENT_TIME},
       |    IF(JSON_CONTAINS_PATH(list_response,'all','$$**.updated_time'), JSON_UNQUOTE(JSON_EXTRACT(list_response,'$$**.updated_time')),'0') as list_first_submit_time,
       |    IF(JSON_CONTAINS_PATH(last_response,'all','$$.updated_time'), JSON_UNQUOTE(JSON_EXTRACT(last_response,'$$.updated_time')),'0') as ${EnjoyedSurveyResponseHelper.LAST_SUBMIT_TIME}
       |FROM $sourceTable
       |WHERE 1=1
       |  AND updated_time >= $fromTime
       |  AND updated_time <= $toTime
       |""".stripMargin
  }
}
