package rever.etl.inquiry.reader

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

import scala.concurrent.duration.DurationInt

class InquiryReader extends SourceReader{
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
  }
  def initQuery(config: Config):String={
    val reportTime = config.getDailyReportTime
    s"""
       |SELECT
       |    object_id as inquiry_id,
       |    IF (JSONHas(snapshot_data,'created_time'),JSONExtractInt(snapshot_data, 'created_time'),JSONExtractInt(snapshot_old_data, 'created_time')) as created_time,
       |    IF (JSONHas(snapshot_data,'updated_time'),JSONExtractInt(snapshot_data, 'updated_time'),JSONExtractInt(snapshot_old_data, 'updated_time')) as updated_time,
       |    IF (JSONHas(snapshot_data,'owner'),JSONExtractString(snapshot_data, 'owner'),JSONExtractString(snapshot_old_data, 'owner')) as owner_id,
       |    IF (JSONHas(snapshot_data,'owner_team'),JSONExtractString(snapshot_data, 'owner_team'),JSONExtractString(snapshot_old_data, 'owner_team')) as owner_team
       |FROM inquiry_1
       |WHERE 1=1
       |LIMIT 0,100
       |""".stripMargin
  }
//  AND timestamp >= $reportTime
//    AND timestamp < ${reportTime + 1.days.toMillis}
}
