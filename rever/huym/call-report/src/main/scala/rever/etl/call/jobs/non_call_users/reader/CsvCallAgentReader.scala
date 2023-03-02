package rever.etl.call.jobs.non_call_users.reader

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

class CsvCallAgentReader extends SourceReader{
  override def read(tableName: String, config: Config): Dataset[Row] = {
    SparkSession.active
      .read
      .format("csv")
      .option("header","true")
      .load("data/User_using_call_19-12-2022.csv")
  }
}
