package rever.etl.inquiry.reader

import org.apache.spark.sql.functions.col
import org.apache.spark.sql.types.{FloatType, IntegerType, LongType}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config

class ListingCsvReader extends SourceReader{

  override def read(s: String, config: Config): Dataset[Row] = {
    SparkSession.active.read
      .format("csv")
      .option("header", value = true)
      .csv(s"data/Listing.csv")
      .withColumn("published_time",col("published_time").cast(LongType))
      .withColumn("updated_time",col("updated_time").cast(LongType))
      .withColumn("num_bed_room",col("num_bed_room").cast(IntegerType))
      .withColumn("sale_price",col("sale_price").cast(FloatType))
      .withColumn("area_using",col("area_using").cast(FloatType))
  }
}
