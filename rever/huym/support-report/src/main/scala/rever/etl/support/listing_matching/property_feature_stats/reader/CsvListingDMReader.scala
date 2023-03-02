package rever.etl.support.listing_matching.property_feature_stats.reader

import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.types.DoubleType
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import rever.rsparkflow.spark.api.SourceReader
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.utils.JsonUtils
import rever.etl.support.domain.ListingFeatureFields
import rever.etl.support.listing_matching.property_feature_stats.FeatureStatsHelper

class CsvListingDMReader extends SourceReader{
  override def read(s: String, config: Config): Dataset[Row] = {
    val df =SparkSession.active.read
      .format("csv")
      .option("header","true")
      .load("data/ListingDf.csv")
      .withColumn(ListingFeatureFields.AREA,col(ListingFeatureFields.AREA).cast(DoubleType))
      .withColumn(ListingFeatureFields.NUM_BED_ROOM,col(ListingFeatureFields.NUM_BED_ROOM).cast(DoubleType))
      .withColumn(ListingFeatureFields.NUM_BATH_ROOM,col(ListingFeatureFields.NUM_BATH_ROOM).cast(DoubleType))
      .withColumn(ListingFeatureFields.SALE_PRICE,col(ListingFeatureFields.SALE_PRICE).cast(DoubleType))
      .withColumn(ListingFeatureFields.RENT_PRICE,col(ListingFeatureFields.RENT_PRICE).cast(DoubleType))

    convertUsdToVndPrice(config,df)
  }
  def convertUsdToVndPrice(config: Config, df:DataFrame) :DataFrame={
    val listingMatchingStatsConfig  = config.getAndDecodeBase64("listing_matching_stats_config")
    val listingMatchingStatsConfigClass = JsonUtils.fromJson[FeatureStatsHelper.ListingMatchingStatsConfig](listingMatchingStatsConfig)
    val usdRate = listingMatchingStatsConfigClass.usdRate
    df
      .withColumn(
        ListingFeatureFields.SALE_PRICE ,
        when(col(ListingFeatureFields.SALE_PRICE_UNIT)==="usd",col(ListingFeatureFields.SALE_PRICE)*usdRate)
          .otherwise(col(ListingFeatureFields.SALE_PRICE))
      )
      .withColumn(
        ListingFeatureFields.RENT_PRICE,
        when(col(ListingFeatureFields.RENT_PRICE_UNIT)==="usd",col(ListingFeatureFields.RENT_PRICE)*usdRate)
          .otherwise(col(ListingFeatureFields.RENT_PRICE))
      )
      .drop(ListingFeatureFields.SALE_PRICE_UNIT)
      .drop(ListingFeatureFields.RENT_PRICE_UNIT)
  }
}
