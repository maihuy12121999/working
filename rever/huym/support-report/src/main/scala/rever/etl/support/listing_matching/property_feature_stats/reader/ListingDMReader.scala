package rever.etl.support.listing_matching.property_feature_stats.reader

import org.apache.spark.sql.functions.{col, when}
import org.apache.spark.sql.{DataFrame, Dataset, Row, SparkSession}
import rever.rsparkflow.spark.api.SourceReader
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.utils.JsonUtils
import rever.etl.support.domain.ListingFeatureFields
import rever.etl.support.listing_matching.property_feature_stats.FeatureStatsHelper.ListingMatchingStatsConfig

class ListingDMReader extends SourceReader {

  /** categorical
    * * architectural_style
    *  *  balcony_direction
    *  * city_id
    *  * direction
    *  * district_id
    *  * furniture_status
    *  * is_hot
    *  * juridical_status
    *  * ownership
    *  * property_type
    *  * service_type
    *  * street_id
    *  * ward_id
    * numerical
    * * area
    * * num_bath_room
    * * num_bed_room
    * * sale_price
    * * rent_price
    *
    * @param s
    * @param config
    * @return
    */
  override def read(s: String, config: Config): Dataset[Row] = {
    val driver = config.get("CH_DRIVER")
    val host = config.get("CH_HOST")
    val port = config.getInt("CH_PORT")
    val userName = config.get("CH_USER_NAME")
    val password = config.get("CH_PASSWORD")
    val df = SparkSession.active.read
      .format("jdbc")
      .option("url", s"jdbc:clickhouse://$host:$port/datamart")
      .option("inferSchema", "true")
      .option("user", userName)
      .option("password", password)
      .option("driver", driver)
      .option("query", buildQuery())
      .load()

    convertUsdToVndPrice(config, df)

  }

  private def convertUsdToVndPrice(config: Config, df: DataFrame): DataFrame = {

    val statsConfig = JsonUtils.fromJson[ListingMatchingStatsConfig](
      config.getAndDecodeBase64("listing_matching_stats_config")
    )

    df
      .withColumn(
        ListingFeatureFields.SALE_PRICE,
        when(
          col(ListingFeatureFields.SALE_PRICE_UNIT) === "usd",
          col(ListingFeatureFields.SALE_PRICE) * statsConfig.usdRate
        )
          .otherwise(col(ListingFeatureFields.SALE_PRICE))
      )
      .withColumn(
        ListingFeatureFields.RENT_PRICE,
        when(
          col(ListingFeatureFields.RENT_PRICE_UNIT) === "usd",
          col(ListingFeatureFields.RENT_PRICE) * statsConfig.usdRate
        )
          .otherwise(col(ListingFeatureFields.RENT_PRICE))
      )
      .drop(ListingFeatureFields.SALE_PRICE_UNIT)
      .drop(ListingFeatureFields.RENT_PRICE_UNIT)
  }

  def buildQuery(): String = {
    """
      |select
      |    toString(architectural_style) as architectural_style,
      |    toString(balcony_direction) as balcony_direction,
      |    toString(direction) as direction,
      |    furniture_status,
      |    toString(is_hot) as is_hot,
      |    toString(juridical_status) as juridical_status,
      |    ownership,
      |    toString(property_type) as property_type,
      |    toString(service_type) as service_type,
      |    JSONExtractString(address,'city_id') as city,
      |    JSONExtractString(address,'district_id') as district,
      |    JSONExtractString(address,'street_id') as street,
      |    JSONExtractString(address,'ward_id') as ward,
      |    toString(JSONExtractInt(transaction,'stage')) as stage,
      |    area,
      |    toFloat64(num_bath_room) as num_bath_room,
      |    toFloat64(num_bed_room) as num_bed_room,
      |    JSONExtract(additional_info,'basic','sale_price','Float64') as sale_price,
      |    JSONExtract(additional_info,'basic','rent_price','Float64') as rent_price,
      |    JSONExtractString(additional_info,'basic','sale_price_unit') as sale_price_unit,
      |    JSONExtractString(additional_info,'basic','rent_price_unit') as rent_price_unit
      |from rever_search_property
      |where 1=1
      |   and property_status != 405
      |""".stripMargin
  }
}
