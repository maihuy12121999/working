package restore_oppo_historical

import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object RestoreOppoHistoricalHelper {
  final val OBJECT_ID = "object_id"
  final val OLD_DATA = "old_data"
  final val DATA = "new_data"
  final val TIMESTAMP = "timestamp"
  final val STAGE_ID = "stage_id"
  final val PIPELINE_ID = "pipeline_id"
  final val ACTION = "action"
  final val PERFORMER = "performer"
  final val SOURCE = "source"
  final val TYPE = "type"
  final val LOG_TIME = "log_time"
  final val ID = "id"

  final val oppoHistoricalRawSchema = StructType(
    Array(
      StructField(OBJECT_ID, StringType, nullable = false),
      StructField("detail_row", StringType, nullable = false)
    )
  )
  final val oppoHistoricalSchema = StructType(
    Array(
      StructField(TIMESTAMP, LongType, nullable = false),
      StructField(OLD_DATA, StringType, nullable = false),
      StructField(DATA, StringType, nullable = false),
      StructField(PERFORMER, StringType, nullable = false),
      StructField(SOURCE, StringType, nullable = false),
      StructField(ACTION, StringType, nullable = false),
      StructField(OBJECT_ID, StringType, nullable = false)
    )
  )

}
case class OppoHistory(
    timestamp: Long,
    oldData: String,
    newData: String,
    performer: String,
    source: String,
    action: String,
    dfType: String
)
