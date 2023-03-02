package rever.etl.emc.tool.hot_lead_export.writer

import org.apache.spark.sql.functions.{col, from_unixtime}
import org.apache.spark.sql.{Dataset, Row}
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config

class HotLeadWriter extends SinkWriter with FlowMixin {
  override def write(tableName: String, df: Dataset[Row], config: Config): Dataset[Row] = {
    val resultDf = df
      .withColumn("first_call_time", from_unixtime(col("first_call_time") / 1000, "dd-MM-yyyy HH:mm:ss"))
      .withColumn("created_time", from_unixtime(col("created_time") / 1000, "dd-MM-yyyy HH:mm:ss"))

    exportA1DfToS3(config.getJobId, System.currentTimeMillis(), resultDf, config)
    df
  }
}
