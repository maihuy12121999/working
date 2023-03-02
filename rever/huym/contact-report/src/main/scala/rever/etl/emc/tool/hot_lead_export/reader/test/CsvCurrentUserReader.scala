package rever.etl.emc.tool.hot_lead_export.reader.test

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.domain.SparkSessionImplicits._

class CsvCurrentUserReader extends SourceReader{
  override def read(tableName: String, config: Config): Dataset[Row] = {
    SparkSession.active
      .read
      .format("csv")
      .option("header","true")
      .load("data/current_user.csv")
      .withColumn("updated_time",col("updated_time").cast(LongType))
      .dropDuplicateCols(Seq("username"),col("updated_time").desc)
      .drop("updated_time")
  }
}
