package rever.etl.engagement.linquiry.reader

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

class InquiryReader extends SourceReader {
  override def read(s: String, config: Config): Dataset[Row] = {
    val host = config.get("CH_HOST")
    val driver = config.get("CH_DRIVER")
    val port = config.get("CH_PORT")
    val userName = config.get("CH_USER_NAME")
    val passWord = config.get("CH_PASSWORD")
    val df = SparkSession.active.read
      .format("jdbc")
      .option("url", s"jdbc:clickhouse://$host:$port/historical")
      .option("user", userName)
      .option("password", passWord)
      .option("driver", driver)
      .option("query", initQuery(config))
      .load()

    df.orderBy(col("timestamp").desc)
      .dropDuplicates("inquiry_id")
  }
  def initQuery(config: Config): String = {
    s"""
       |SELECT
       |    toString(object_id) as inquiry_id,
       |    IF (JSONHas(snapshot_data,'created_time'),JSONExtract(snapshot_data, 'created_time', 'Int64'),JSONExtract(snapshot_old_data, 'created_time', 'Int64')) as created_time,
       |    IF (JSONHas(snapshot_data,'updated_time'),JSONExtract(snapshot_data, 'updated_time', 'Int64'),JSONExtract(snapshot_old_data, 'updated_time', 'Int64')) as updated_time,
       |    IF (JSONHas(snapshot_data,'owner'),JSONExtractString(snapshot_data, 'owner'),JSONExtractString(snapshot_old_data, 'owner')) as owner_id,
       |    IF (JSONHas(snapshot_data,'owner_team'),JSONExtractString(snapshot_data, 'owner_team'),JSONExtractString(snapshot_old_data, 'owner_team')) as owner_team,
       |    timestamp
       |FROM inquiry_1
       |WHERE 1=1
       |""".stripMargin
  }
}
