package cta_call_analysis.writer

import org.apache.spark.sql.{DataFrame, Dataset, Row}
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config

/**
 * @author anhlt (andy)
 * @since 14/07/2022
**/
 class TestCallHistoryWriter extends SinkWriter{
   override def write(tableName: String, tableData: Dataset[Row], config: Config): Dataset[Row] = {
     tableData.show(20)
     tableData
   }
}
