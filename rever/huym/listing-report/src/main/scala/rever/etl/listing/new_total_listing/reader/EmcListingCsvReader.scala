package rever.etl.listing.new_total_listing.reader

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{IntegerType, LongType, StringType}
import rever.etl.listing.new_total_listing.EmcListingHelper
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

class EmcListingCsvReader extends SourceReader{
  override def read(tableName: String, config: Config): Dataset[Row] = {

    val df = SparkSession.active
      .read
      .format("csv")
      .option("header","true")
      .load("data/listing0-50.csv")
      .withColumn(EmcListingHelper.TIMESTAMP,col(EmcListingHelper.TIMESTAMP).cast(LongType))
    df
  }
}
