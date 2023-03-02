package rever.rsparkflow.spark.example.writer

import org.apache.spark.sql.DataFrame
import rever.rsparkflow.spark.FlowMixin
import rever.rsparkflow.spark.api.SinkWriter
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.utils.PathExtension

class SimpleWriter extends SinkWriter with FlowMixin {
  def write(
      tableName: String,
      dataFrame: DataFrame,
      config: Config
  ): DataFrame = {

    exportA1DfToS3(
      config.getJobId,
      config.getDailyReportTime,
      dataFrame,
      config,
      Some(tableName),
      extension = PathExtension.CSV
    )
    dataFrame
  }
}
