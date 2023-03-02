package rever.etl.engagement.note.reader

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.engagement.domain.NoteFields
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

import scala.concurrent.duration.DurationInt

class NoteReader extends SourceReader{
  override def read(tableName: String, config: Config): Dataset[Row] = {
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
      .drop(NoteFields.TYPE,NoteFields.CONTACTS)
  }

  def initQuery(config: Config):String ={
    val reportTime = config.getDailyReportTime
    s"""
       |SELECT
       |        IF (JSONHas(data,'type'),JSONExtractString(data, 'type'),JSONExtractString(old_data, 'type')) as ${NoteFields.TYPE},
       |        IF (JSONHas(data,'owner_market_center'),JSONExtractString(data, 'owner_market_center'),JSONExtractString(old_data, 'owner_market_center')) as ${NoteFields.MARKET_CENTER_ID},
       |        IF (JSONHas(data,'owner_team'),JSONExtractString(data,'owner_team'),JSONExtractString(old_data,'owner_team')) as ${NoteFields.TEAM_ID},
       |        IF (JSONHas(data,'owner'),JSONExtractString(data,'owner'),JSONExtractString(old_data,'owner')) as ${NoteFields.AGENT_ID},
       |        IF (JSONHas(data,'source'),JSONExtractString(data,'source'),JSONExtractString(old_data,'source')) as ${NoteFields.SOURCE},
       |        IF (JSONHas(data, 'associations', 'contacts'), JSONExtractRaw(COALESCE(data, '{}'), 'associations', 'contacts'), JSONExtractRaw(COALESCE(old_data, '{}'), 'associations', 'contacts')) as contacts,
       |        toString(arrayDistinct(arrayMap(i -> JSONExtractString(contacts, i + 1, 'cid'), range(JSONLength(contacts))))) as ${NoteFields.CIDS},
       |        toString(IF (JSONHas(data, 'status'),JSONExtractInt(data,'status'),JSONExtractInt(old_data,'status'))) as ${NoteFields.STATUS},
       |        object_id as ${NoteFields.NOTE_ID},
       |        toInt64(${NoteFields.TIMESTAMP}) as ${NoteFields.TIMESTAMP}
       |FROM engagement_historical_1
       |WHERE 1=1
       |    AND ${NoteFields.TYPE} = 'note'
       |    AND ${NoteFields.TIMESTAMP}>=$reportTime
       |    AND ${NoteFields.TIMESTAMP}<$reportTime+${1.days.toMillis}
       |""".stripMargin
  }

}
