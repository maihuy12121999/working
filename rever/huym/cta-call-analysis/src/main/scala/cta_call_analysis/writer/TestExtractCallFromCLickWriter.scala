package cta_call_analysis.writer

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{Dataset, Row}
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config

class TestExtractCallFromCLickWriter extends SinkWriter{
  override def write(tableName: String, tableData: Dataset[Row], config: Config): Dataset[Row] = {
    tableData
      .where(col("from")=!="")
//      .where(col("phone_number")=!="")
      .show(20,truncate = false)
    tableData
  }
}
