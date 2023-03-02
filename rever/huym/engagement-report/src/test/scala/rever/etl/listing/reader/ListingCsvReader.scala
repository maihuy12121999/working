package rever.etl.listing.reader

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.listing.config.UpdatedListingField
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

class ListingCsvReader extends SourceReader {
  override def read(tableName: String, config: Config): Dataset[Row] = {
    SparkSession.active.read
      .format("csv")
      .option("header", value = true)
      .csv(s"data/Listing.csv")
      .withColumn(UpdatedListingField.UPDATED_STATUS_TIME, col(UpdatedListingField.UPDATED_STATUS_TIME).cast(LongType))
  }
}
