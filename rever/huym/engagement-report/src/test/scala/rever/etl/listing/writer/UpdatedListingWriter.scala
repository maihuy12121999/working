package rever.etl.listing.writer

import org.apache.spark.sql.functions.{col, from_unixtime, to_date}
import org.apache.spark.sql.types.StringType
import org.apache.spark.sql.{Dataset, Row, SaveMode}
import rever.etl.listing.config.UpdatedListingField
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config

class UpdatedListingWriter extends SinkWriter{
  override def write(tableName: String, df: Dataset[Row], config: Config): Dataset[Row] = {
    df
      .withColumn(UpdatedListingField.UPDATED_TIME,to_date(from_unixtime(col(UpdatedListingField.UPDATED_TIME)/1000),"yyyy-MM-dd HH:mm:ss"))
      .withColumn(UpdatedListingField.UPDATED_STATUS_TIME,to_date(from_unixtime(col(UpdatedListingField.UPDATED_STATUS_TIME)/1000),"yyyy-MM-dd HH:mm:ss"))
      .coalesce(1)
      .write
      .mode(SaveMode.Overwrite)
      .format("csv")
      .option("header",value = true)
      .csv("output/updated_listing")
    df
  }
}
