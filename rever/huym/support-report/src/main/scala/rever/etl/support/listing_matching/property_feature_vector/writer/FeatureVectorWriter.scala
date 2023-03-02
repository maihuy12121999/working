package rever.etl.support.listing_matching.property_feature_vector.writer

import org.apache.spark.sql.{Dataset, Row, SparkSession}
import rever.rsparkflow.spark.api.SinkWriter
import rever.rsparkflow.spark.api.configuration.Config
import rever.rsparkflow.spark.utils.JsonUtils
import rever.etl.support.client.{ListingMatchingClient, ListingMatchingVectorConfig}
import rever.etl.support.domain.CreateListingFeatureVectorRecord

class FeatureVectorWriter extends SinkWriter {
  override def write(tableName: String, df: Dataset[Row], config: Config): Dataset[Row] = {
    df.printSchema()
    val featureVectorConfig = JsonUtils.fromJson[ListingMatchingVectorConfig](
      config.getAndDecodeBase64("listing_matching_vector_config")
    )

    val batchCounter = SparkSession.active.sparkContext.longAccumulator("batchCounter")
    df.rdd
      .mapPartitions(_.grouped(featureVectorConfig.batchSize.getOrElse(200)))
      .foreach(rows => {
        val client = ListingMatchingClient.client(featureVectorConfig)

        val numIngestedRows = client.insertListingVectors(toCreateFeatureVectorRecords(rows))

        batchCounter.add(numIngestedRows)
      })

    println(s"Created and inserted ${batchCounter.value} records.")

    df
  }

  private def toCreateFeatureVectorRecords(rows: Seq[Row]): Seq[CreateListingFeatureVectorRecord] = {
    rows.map(row => {
      CreateListingFeatureVectorRecord(
        Map(
          CreateListingFeatureVectorRecord.PROPERTY_ID -> row.getAs[String](
            CreateListingFeatureVectorRecord.PROPERTY_ID
          ),
          CreateListingFeatureVectorRecord.NUM_BED_ROOM -> row.getAs[Int](
            CreateListingFeatureVectorRecord.NUM_BED_ROOM
          ),
          CreateListingFeatureVectorRecord.NUM_BATH_ROOM -> row.getAs[Int](
            CreateListingFeatureVectorRecord.NUM_BATH_ROOM
          ),
          CreateListingFeatureVectorRecord.AREA -> row.getAs[Double](CreateListingFeatureVectorRecord.AREA),
          CreateListingFeatureVectorRecord.SERVICE_TYPE -> row.getAs[Int](
            CreateListingFeatureVectorRecord.SERVICE_TYPE
          ),
          CreateListingFeatureVectorRecord.ADDRESS -> JsonUtils.fromJson[Map[String, Any]](
            row.getAs[String](CreateListingFeatureVectorRecord.ADDRESS)
          ),
          CreateListingFeatureVectorRecord.PROPERTY_TYPE -> row.getAs[Int](
            CreateListingFeatureVectorRecord.PROPERTY_TYPE
          ),
          CreateListingFeatureVectorRecord.IS_HOT -> row.getAs[Int](CreateListingFeatureVectorRecord.IS_HOT),
          CreateListingFeatureVectorRecord.OWNERSHIP -> row.getAs[String](CreateListingFeatureVectorRecord.OWNERSHIP),
          CreateListingFeatureVectorRecord.ARCHITECTURAL_STYLE -> row.getAs[Int](
            CreateListingFeatureVectorRecord.ARCHITECTURAL_STYLE
          ),
          CreateListingFeatureVectorRecord.DIRECTION -> row.getAs[Int](CreateListingFeatureVectorRecord.DIRECTION),
          CreateListingFeatureVectorRecord.BALCONY_DIRECTION -> row.getAs[Int](
            CreateListingFeatureVectorRecord.BALCONY_DIRECTION
          ),
          CreateListingFeatureVectorRecord.ADDITIONAL_INFO -> JsonUtils.fromJson[Map[String, Any]](
            row.getAs[String](CreateListingFeatureVectorRecord.ADDITIONAL_INFO)
          ),
          CreateListingFeatureVectorRecord.JURIDICAL_STATUS -> row.getAs[Int](
            CreateListingFeatureVectorRecord.JURIDICAL_STATUS
          ),
          CreateListingFeatureVectorRecord.FURNITURE_STATUS -> row.getAs[String](
            CreateListingFeatureVectorRecord.FURNITURE_STATUS
          ),
          CreateListingFeatureVectorRecord.HOUSE_STATUS -> row.getAs[Int](
            CreateListingFeatureVectorRecord.HOUSE_STATUS
          ),
          CreateListingFeatureVectorRecord.ALLEYWAY_WIDTH -> row
            .getAs[Int](CreateListingFeatureVectorRecord.ALLEYWAY_WIDTH)
            .toString,
          CreateListingFeatureVectorRecord.PROPERTY_STATUS -> row.getAs[Int](
            CreateListingFeatureVectorRecord.PROPERTY_STATUS
          ),
          CreateListingFeatureVectorRecord.SALE_DISCOUNT_VND -> row.getAs[Float](
            CreateListingFeatureVectorRecord.SALE_DISCOUNT_VND
          ),
          CreateListingFeatureVectorRecord.RENT_DISCOUNT_VND -> row.getAs[Float](
            CreateListingFeatureVectorRecord.RENT_DISCOUNT_VND
          ),
          CreateListingFeatureVectorRecord.PUBLISHED_TIME -> row.getAs[Long](
            CreateListingFeatureVectorRecord.PUBLISHED_TIME
          ),
          CreateListingFeatureVectorRecord.UPDATED_TIME -> row.getAs[Long](
            CreateListingFeatureVectorRecord.UPDATED_TIME
          ),
          CreateListingFeatureVectorRecord.TRANSACTION -> JsonUtils.fromJson[Map[String, Any]](
            row.getAs[String](CreateListingFeatureVectorRecord.TRANSACTION)
          ),
          CreateListingFeatureVectorRecord.SALE_PRICE_VND -> row.getAs[Float](
            CreateListingFeatureVectorRecord.SALE_PRICE_VND
          ),
          CreateListingFeatureVectorRecord.RENT_PRICE_VND -> row.getAs[Float](
            CreateListingFeatureVectorRecord.RENT_PRICE_VND
          )
        ),
        "rever"
      )
    })
  }

}
