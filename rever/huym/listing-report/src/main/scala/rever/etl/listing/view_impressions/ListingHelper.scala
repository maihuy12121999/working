package rever.etl.listing.view_impressions

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object ListingHelper {

  final val A1 = "a1"
  final val A7 = "a7"
  final val A30 = "a30"
  final val A0 = "a0"
  final val An = "an"

  final val finalSchema = StructType(
    Array(
      StructField("domain", DataTypes.StringType, nullable = false),
      StructField("property_id", DataTypes.StringType, nullable = false),
      StructField(A1, DataTypes.LongType, nullable = false),
      StructField(A7, DataTypes.LongType, nullable = false),
      StructField(A30, DataTypes.LongType, nullable = false),
      StructField(A0, DataTypes.LongType, nullable = false)
    )
  )
}
