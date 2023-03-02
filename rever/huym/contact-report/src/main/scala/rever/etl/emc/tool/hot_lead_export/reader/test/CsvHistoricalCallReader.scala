package rever.etl.emc.tool.hot_lead_export.reader.test

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.emc.tool.hot_lead_export.domain.HotLeadFields

class CsvHistoricalCallReader extends SourceReader {
  override def read(tableName: String, config: Config): Dataset[Row] = {
    SparkSession.active
      .read
      .format("csv")
      .option("header", "true")
      .load("data/call_history.csv")
      .withColumn(HotLeadFields.CALL_TIME,col(HotLeadFields.CALL_TIME).cast(LongType))
      .withColumn(HotLeadFields.DURATION,col(HotLeadFields.DURATION).cast(LongType))
  }
}
