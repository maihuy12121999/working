package rever.etl.listing.reader

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.utils.TimestampUtils

class UpdatedListingReader extends SourceReader {
  override def read(df: String, config: Config): Dataset[Row] = {
    val host = config.get("CH_HOST")
    val driver = config.get("CH_DRIVER")
    val port = config.get("CH_PORT")
    val userName = config.get("CH_USER_NAME")
    val passWord = config.get("CH_PASSWORD")
    SparkSession.active.read
      .format("jdbc")
      .option("url", s"jdbc:clickhouse://$host:$port/historical")
      .option("user", userName)
      .option("password", passWord)
      .option("driver", driver)
      .option("query", initQuery(config))
      .load()
  }
  def initQuery(config: Config): String = {
    val startDate = TimestampUtils.parseMillsFromString(config.get("RV_START_DATE", "2022-07-01"), "yyyy-MM-dd")
    val endDate = TimestampUtils.parseMillsFromString(config.get("RV_END_DATE", "2022-09-20"), "yyyy-MM-dd")
    s"""
       |SELECT
       |    IF (JSONHas(data,'owner_id'),JSONExtractString(data,'owner_id'),JSONExtractString(old_data,'owner_id')) as owner_id,
       |    IF (JSONHas(data,'rever_id'),JSONExtractString(data,'rever_id'),JSONExtractString(old_data,'rever_id')) as rever_id,
       |    IF (JSONHas(data,'updated_time'),JSONExtractInt(data,'updated_time'),JSONExtractInt(old_data,'updated_time')) as updated_time,
       |    data,
       |    old_data,
       |    object_id as listing_id
       |FROM es_rever_property_1
       |WHERE 1=1
       |    AND action ='UPDATE'
       |    AND timestamp >=$startDate
       |    AND timestamp <= $endDate
       |""".stripMargin

  }


}
