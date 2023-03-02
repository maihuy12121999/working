package rever.etl.agentcrm.active_user

import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import rever.etl.agentcrm.config.FieldConfig
import rever.etl.rsparkflow.utils.Utils

object ActiveUserHelper {

  final val TOTAL_USERS = "total_user"
  final val TOTAL_ACTIONS = "total_actions"
  final val A1 = "a1"
  final val TOTAL_A1_ACTIONS = "total_a1_actions"
  final val A7 = "a7"
  final val TOTAL_A7_ACTIONS = "total_a7_actions"
  final val A30 = "a30"
  final val TOTAL_A30_ACTIONS = "total_a30_actions"
  final val AN = "an"
  final val TOTAL_AN_ACTIONS = "total_an_actions"
  final val A0 = "a0"
  final val TOTAL_A0_ACTIONS = "total_a0_actions"

  final val schema = StructType(
    Array(
      StructField(FieldConfig.DATE, LongType, nullable = false),
      StructField(FieldConfig.DEPARTMENT, StringType, nullable = false),
      StructField(FieldConfig.CLIENT_PLATFORM, StringType, nullable = false),
      StructField(FieldConfig.CLIENT_OS, StringType, nullable = false),
      StructField(FieldConfig.PAGE, StringType, nullable = false),
      StructField(FieldConfig.EVENT, StringType, nullable = false),
      StructField(FieldConfig.USER_ID, StringType, nullable = false)
    )
  )

  final val a7Schema = StructType(
    Array(
      StructField(FieldConfig.DATE, LongType, nullable = false),
      StructField(FieldConfig.DEPARTMENT, StringType, nullable = false),
      StructField(FieldConfig.CLIENT_PLATFORM, StringType, nullable = false),
      StructField(FieldConfig.CLIENT_OS, StringType, nullable = false),
      StructField(FieldConfig.PAGE, StringType, nullable = false),
      StructField(FieldConfig.EVENT, StringType, nullable = false),
      StructField(A7, LongType, nullable = false),
      StructField(TOTAL_A7_ACTIONS, LongType, nullable = false)
    )
  )

  final val a30Schema = StructType(
    Array(
      StructField(FieldConfig.DATE, LongType, nullable = false),
      StructField(FieldConfig.DEPARTMENT, StringType, nullable = false),
      StructField(FieldConfig.CLIENT_PLATFORM, StringType, nullable = false),
      StructField(FieldConfig.CLIENT_OS, StringType, nullable = false),
      StructField(FieldConfig.PAGE, StringType, nullable = false),
      StructField(FieldConfig.EVENT, StringType, nullable = false),
      StructField(A30, LongType, nullable = false),
      StructField(TOTAL_A30_ACTIONS, LongType, nullable = false)
    )
  )
  final val anSchema = StructType(
    Array(
      StructField(FieldConfig.DATE, LongType, nullable = false),
      StructField(FieldConfig.DEPARTMENT, StringType, nullable = false),
      StructField(FieldConfig.CLIENT_PLATFORM, StringType, nullable = false),
      StructField(FieldConfig.CLIENT_OS, StringType, nullable = false),
      StructField(FieldConfig.PAGE, StringType, nullable = false),
      StructField(FieldConfig.EVENT, StringType, nullable = false),
      StructField(AN, LongType, nullable = false),
      StructField(TOTAL_AN_ACTIONS, LongType, nullable = false)
    )
  )

  final val resultSchema = StructType(
    Array(
      StructField(FieldConfig.DATE, LongType, nullable = false),
      StructField(FieldConfig.DEPARTMENT, StringType, nullable = false),
      StructField(FieldConfig.CLIENT_PLATFORM, StringType, nullable = false),
      StructField(FieldConfig.CLIENT_OS, StringType, nullable = false),
      StructField(FieldConfig.PAGE, StringType, nullable = false),
      StructField(FieldConfig.EVENT, StringType, nullable = false),
      StructField(A1, LongType, nullable = false),
      StructField(TOTAL_A1_ACTIONS, LongType, nullable = false),
      StructField(A7, LongType, nullable = false),
      StructField(TOTAL_A7_ACTIONS, LongType, nullable = false),
      StructField(A30, LongType, nullable = false),
      StructField(TOTAL_A30_ACTIONS, LongType, nullable = false),
      StructField(AN, LongType, nullable = false),
      StructField(TOTAL_AN_ACTIONS, LongType, nullable = false),
      StructField(A0, LongType, nullable = false),
      StructField(TOTAL_A0_ACTIONS, LongType, nullable = false)
    )
  )

  def generateRow(row: Row): Seq[Row] = {
    val cartesianRows = Utils.cartesianProduct(
      List(
        List(row.getAs[Long](FieldConfig.DATE)),
        List(row.getAs[String](FieldConfig.DEPARTMENT), "all"),
        List(row.getAs[String](FieldConfig.CLIENT_PLATFORM), "all"),
        List(row.getAs[String](FieldConfig.CLIENT_OS), "all"),
        List(row.getAs[String](FieldConfig.PAGE), "all"),
        List(row.getAs[String](FieldConfig.EVENT), "all"),
        List(row.getAs[String](FieldConfig.USER_ID)),
        List(row.getAs[Long](TOTAL_ACTIONS))
      )
    )
    cartesianRows.map(Row.fromSeq(_))
  }
}
