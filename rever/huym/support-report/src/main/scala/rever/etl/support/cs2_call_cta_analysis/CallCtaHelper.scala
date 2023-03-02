package rever.etl.support.cs2_call_cta_analysis

import org.apache.spark.sql.types.{DataTypes, StructField, StructType}

object CallCtaHelper {
  final val SOURCE_TYPE_LISTING = "listing"
  final val SOURCE_TYPE_PROJECT = "project"
  final val SOURCE_TYPE_PROFILE = "profile"
  final val SOURCE_TYPE_UNKNOWN = "unknown"

  final val callExtensionSchema = StructType(
    Array(
      StructField("assigned_time", DataTypes.LongType, nullable = false),
      StructField("extension", DataTypes.StringType, nullable = false),
      StructField("call_service", DataTypes.StringType, nullable = false),
      StructField("phone_number", DataTypes.StringType, nullable = false),
      StructField("creator", DataTypes.StringType, nullable = false)

    )
  )

  final val callCtaEventSchema = StructType(
    Array(
      StructField("timestamp", DataTypes.LongType, nullable = false),
      StructField("message_id", DataTypes.StringType, nullable = false),
      StructField("anonymous_id", DataTypes.StringType, nullable = false),
      StructField("user_id", DataTypes.StringType, nullable = false),
      StructField("user_phone_number", DataTypes.StringType, nullable = false),
      StructField("url", DataTypes.StringType, nullable = false),
      StructField("source_type", DataTypes.StringType, nullable = false),
      StructField("source_code", DataTypes.StringType, nullable = false),
      StructField("owner", DataTypes.StringType, nullable = false),
      StructField("event", DataTypes.StringType, nullable = false),
      StructField("os", DataTypes.StringType, nullable = false),
      StructField("platform", DataTypes.StringType, nullable = false)
    )
  )

  final val callCtaAnalysisSchema = StructType(
    Array(
      StructField("clicked_at", DataTypes.LongType, nullable = false),
      StructField("message_id", DataTypes.StringType, nullable = false),
      StructField("anonymous_id", DataTypes.StringType, nullable = false),
      StructField("user_id", DataTypes.StringType, nullable = false),
      StructField("user_phone_number", DataTypes.StringType, nullable = false),
      StructField("url", DataTypes.StringType, nullable = false),
      StructField("source_type", DataTypes.StringType, nullable = false),
      StructField("source_code", DataTypes.StringType, nullable = false),
      StructField("event", DataTypes.StringType, nullable = false),
      StructField("os", DataTypes.StringType, nullable = false),
      StructField("platform", DataTypes.StringType, nullable = false),
      StructField("call_id", DataTypes.StringType, nullable = false),
      StructField("caller_phone", DataTypes.StringType, nullable = false),
      StructField("receiver_phone", DataTypes.StringType, nullable = false),
      StructField("extension", DataTypes.StringType, nullable = false),
      StructField("call_status", DataTypes.StringType, nullable = false),
      StructField("duration", DataTypes.IntegerType, nullable = false),
      StructField("call_start_at", DataTypes.LongType, nullable = true),
      StructField("call_end_at", DataTypes.LongType, nullable = true),
      StructField("click_and_call_duration_gap", DataTypes.LongType, nullable = false)
    )
  )
}
