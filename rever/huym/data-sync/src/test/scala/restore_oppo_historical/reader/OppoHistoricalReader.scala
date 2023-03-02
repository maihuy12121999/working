package restore_oppo_historical.reader

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import restore_oppo_historical.RestoreOppoHistoricalHelper
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

class OppoHistoricalReader extends SourceReader {
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

    s"""
       |SELECT
       |    'oppo_historical' as ${RestoreOppoHistoricalHelper.TYPE},
       |    timestamp as ${RestoreOppoHistoricalHelper.TIMESTAMP},
       |    object_id as ${RestoreOppoHistoricalHelper.OBJECT_ID},
       |    data as ${RestoreOppoHistoricalHelper.DATA},
       |    old_data as ${RestoreOppoHistoricalHelper.OLD_DATA},
       |    action as ${RestoreOppoHistoricalHelper.ACTION},
       |    performer as ${RestoreOppoHistoricalHelper.PERFORMER},
       |    source as ${RestoreOppoHistoricalHelper.SOURCE}
       |FROM sale_pipeline_oppo_historical_1
       |WHERE 1=1
       |""".stripMargin
  }

}
