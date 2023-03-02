package rever.etl.support.listing_matching.property_feature_vector.reader

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.rsparkflow.spark.api.SourceReader
import rever.rsparkflow.spark.api.configuration.Config

class CsvListingDMReader extends SourceReader{
  override def read(tableName: String, config: Config): Dataset[Row] = {
    SparkSession.active.read
      .format("csv")
      .options(Map("header"->"true","escape"->"\"","quoteMode"->"\"","inferSchema"->"true"))
      .load("data/ListingDf_fv_1.csv")
  }
}
