package rever.etl.inquiry.writer

import org.apache.spark.sql.{Dataset, Row}
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config

class InquiryWriter extends SinkWriter{
  override def write(tableName: String, df: Dataset[Row], config: Config): Dataset[Row] = {
    df.show(false)
    df
  }
}
