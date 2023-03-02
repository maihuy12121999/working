package rever.etl.support.listing_matching.property_feature_stats

import org.apache.spark.sql.types.{DoubleType, StringType, StructField, StructType}

object FeatureStatsHelper {

  case class NumericalValidation(from: Option[Double], to: Option[Double])

  case class ListingMatchingStatsConfig(numericalValidation: Map[String, NumericalValidation], usdRate: Double)

  final val FIELD = "field"
  final val MIN_VALUE = "min_value"
  final val MAX_VALUE = "max_value"
  final val MEAN_VALUE = "mean_value"
  final val UNIQUE_VALUES = "unique_values"
  final val LOG_TIME = "log_time"
  final val MODEL_VERSION = "model_version"

  final val flattenedCatDfSchema = StructType(
    Array(
      StructField("field", StringType, nullable = false),
      StructField("value", StringType, nullable = false)
    )
  )
  final val flattenedNumDfSchema = StructType(
    Array(
      StructField("field", StringType, nullable = false),
      StructField("value", DoubleType, nullable = false)
    )
  )

}
