package rever.etl.listing.published_listings.reader

import org.apache.spark.sql.functions.{callUDF, col, lit}
import org.apache.spark.sql.types.LongType
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.etl.listing.domain.PublishedListingFields
import rever.etl.rsparkflow.api.SourceReader
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.api.udf.RUdfUtils
class CsvListingActionReader extends SourceReader{
  override def read(tableName: String, config: Config): Dataset[Row] = {
    val df = SparkSession.active
      .read
      .format("csv")
      .option("header","true")
      .load("data/listing_action_reader.csv")
      .withColumn(
        PublishedListingFields.BUSINESS_UNIT,
        callUDF(
          RUdfUtils.RV_DETECT_BUSINESS_UNIT,
          lit(""),
          col(PublishedListingFields.MARKET_CENTER_NAME),
          col(PublishedListingFields.TEAM_NAME)
        )
      )
      .withColumn(PublishedListingFields.TIMESTAMP, col(PublishedListingFields.TIMESTAMP).cast(LongType))
      .drop(PublishedListingFields.MARKET_CENTER_NAME, PublishedListingFields.TEAM_NAME)

    df
  }
}
