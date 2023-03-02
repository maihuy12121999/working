package cta_call_analysis.reader

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

class CsvCallHistoryReader extends SourceReader {
  override def read(tableName: String, config: Config): Dataset[Row] = {
    SparkSession.active.read
      .format("csv")
      .option("header", value = true)
        .csv(s"input/Call_History_Data.csv")
  }
}
