package rever.etl.engagement.meeting

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import org.apache.spark.sql.{DataFrame, Row}
import rever.etl.engagement.domain.MeetingFields
import rever.etl.rsparkflow.utils.Utils

object MeetingHelper {
  final val addCIdsColumnUdf: UserDefinedFunction = udf((cidString: String) => {
    val cIds = cidString.drop(1).dropRight(1).split(",")
    cIds.map(_.drop(1).dropRight(1))
  })
  def standardizeStringCols(df: DataFrame, cols: Seq[String], defaultValue: String): DataFrame = {
    df.na.fill(defaultValue).na.replace(cols, Map("" -> defaultValue))
  }
  def generateMeetingRow(row: Row): Seq[Row] = {
    val cartesianProductList = Utils.cartesianProduct(
      List(
        List(row.getAs[Long](MeetingFields.DATE)),
        List(row.getAs[String](MeetingFields.INTERVAL)),
        List(row.getAs[String](MeetingFields.BUSINESS_UNIT), "all"),
        List(row.getAs[String](MeetingFields.MARKET_CENTER_ID), "all"),
        List(row.getAs[String](MeetingFields.TEAM_ID), "all"),
        List(row.getAs[String](MeetingFields.USER_TYPE)),
        List(row.getAs[String](MeetingFields.AGENT_ID)),
        List(row.getAs[String](MeetingFields.SOURCE)),
        List(row.getAs[String](MeetingFields.CID), "all"),
        List(row.getAs[String](MeetingFields.STATUS)),
        List(row.getAs[String](MeetingFields.MEETING_ID)),
        List(row.getAs[Long](MeetingFields.DURATION))
      )
    )
    cartesianProductList.map(r => Row.fromSeq(r))
  }
  def definedSchema: StructType = {
    StructType(
      Array(
        StructField(MeetingFields.DATE, LongType, true),
        StructField(MeetingFields.INTERVAL, StringType, true),
        StructField(MeetingFields.BUSINESS_UNIT, StringType, true),
        StructField(MeetingFields.MARKET_CENTER_ID, StringType, true),
        StructField(MeetingFields.TEAM_ID, StringType, true),
        StructField(MeetingFields.USER_TYPE, StringType, true),
        StructField(MeetingFields.AGENT_ID, StringType, true),
        StructField(MeetingFields.SOURCE, StringType, true),
        StructField(MeetingFields.CID, StringType, true),
        StructField(MeetingFields.STATUS, StringType, true),
        StructField(MeetingFields.MEETING_ID, StringType, true),
        StructField(MeetingFields.DURATION, LongType, true)
      )
    )
  }
}
