package rever.etl.engagement.meeting.reader

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.engagement.domain.MeetingFields
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

import scala.concurrent.duration.DurationInt

class MeetingReader extends SourceReader{
  override def read(s: String, config: Config): Dataset[Row] = {
    val host = config.get("CH_HOST")
    val driver = config.get("CH_DRIVER")
    val port = config.get("CH_PORT")
    val userName = config.get("CH_USER_NAME")
    val passWord = config.get("CH_PASSWORD")
    SparkSession.active.read
      .format("jdbc")
      .option("url",s"jdbc:clickhouse://$host:$port/historical")
      .option("user",userName)
      .option("password",passWord)
      .option("driver",driver)
      .option("query",initQuery(config))
      .load()
      .drop("type","start_time","end_time","contacts")
  }
  def initQuery(config: Config):String ={
    val reportTime = config.getDailyReportTime
    s"""
      |SELECT
      |        IF (JSONHas(data,'type'),JSONExtractString(data, 'type'),JSONExtractString(old_data, 'type')) as ${MeetingFields.TYPE},
      |        IF (JSONHas(data,'owner_market_center'),JSONExtractString(data, 'owner_market_center'),JSONExtractString(old_data, 'owner_market_center')) as ${MeetingFields.MARKET_CENTER_ID},
      |        IF (JSONHas(data,'owner_team'),JSONExtractString(data,'owner_team'),JSONExtractString(old_data,'owner_team')) as ${MeetingFields.TEAM_ID},
      |        IF (JSONHas(data,'owner'),JSONExtractString(data,'owner'),JSONExtractString(old_data,'owner')) as ${MeetingFields.AGENT_ID},
      |        IF (JSONHas(data,'source'),JSONExtractString(data,'source'),JSONExtractString(old_data,'source')) as ${MeetingFields.SOURCE},
      |        IF (JSONHas(data, 'associations', 'contacts'), JSONExtractRaw(COALESCE(data, '{}'), 'associations', 'contacts'), JSONExtractRaw(COALESCE(old_data, '{}'), 'associations', 'contacts')) as contacts,
      |        toString(arrayDistinct(arrayMap(i -> JSONExtractString(contacts, i + 1, 'cid'), range(JSONLength(contacts))))) as ${MeetingFields.CID},
      |        toString(IF (JSONHas(data, 'status'),JSONExtractInt(data,'status'),JSONExtractInt(old_data,'status'))) as ${MeetingFields.STATUS},
      |        object_id as ${MeetingFields.MEETING_ID},
      |        IF (JSONHas(data,'meeting_metadata','start_time'),JSONExtractInt(data,'meeting_metadata','start_time'),JSONExtractInt(old_data,'meeting_metadata','start_time')) as ${MeetingFields.START_TIME},
      |        IF (JSONHas(data,'meeting_metadata','end_time'),JSONExtractInt(data,'meeting_metadata','end_time'),JSONExtractInt(old_data,'meeting_metadata','end_time')) as ${MeetingFields.END_TIME},
      |        toInt64(end_time - start_time) as ${MeetingFields.DURATION},
      |        ${MeetingFields.START_TIME} as ${MeetingFields.TIMESTAMP}
      |FROM engagement_historical_1
      |WHERE 1=1
      |    AND ${MeetingFields.TYPE} = 'meeting'
      |    AND ${MeetingFields.TIMESTAMP}>=$reportTime
      |    AND ${MeetingFields.TIMESTAMP}<$reportTime+${1.days.toMillis}
      |""".stripMargin
  }
}
