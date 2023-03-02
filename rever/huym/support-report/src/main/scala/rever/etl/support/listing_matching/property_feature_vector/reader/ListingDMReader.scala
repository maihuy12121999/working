package rever.etl.support.listing_matching.property_feature_vector.reader

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.rsparkflow.spark.api.SourceReader
import rever.rsparkflow.spark.api.configuration.Config

class ListingDMReader extends SourceReader{


  override def read(tableName: String, config: Config): Dataset[Row] = {
    val driver = config.get("CH_DRIVER")
    val host = config.get("CH_HOST")
    val port = config.getInt("CH_PORT")
    val userName = config.get("CH_USER_NAME")
    val password = config.get("CH_PASSWORD")
    SparkSession.active.read
      .format("jdbc")
      .option("url", s"jdbc:clickhouse://$host:$port/datamart")
      .option("inferSchema", "true")
      .option("user", userName)
      .option("password", password)
      .option("driver", driver)
      .option("query", buildQuery())
      .load()


  }
  def buildQuery():String={
    s"""
       |select
       |    property_id,
       |    num_bed_room,
       |    num_bath_room,
       |    area,
       |    service_type,
       |    address,
       |    is_hot,
       |    ownership,
       |    architectural_style,
       |    direction,
       |    balcony_direction,
       |    additional_info,
       |    juridical_status,
       |    furniture_status,
       |    house_status,
       |    alleyway_width,
       |    sale_discount_vnd,
       |    rent_discount_vnd,
       |    'rever' as listing_type,
       |    property_status,
       |    property_type,
       |    published_time,
       |    updated_time,
       |    JSONExtractInt(transaction,'stage') as stage,
       |    transaction,
       |    sale_price_vnd,
       |    rent_price_vnd
       |from rever_search_property
       |""".stripMargin
  }

}
