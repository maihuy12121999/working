package rever.etl.engagement.note

import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import rever.etl.engagement.domain.NoteFields
import rever.etl.rsparkflow.utils.Utils

object NoteHelper {
  def generateProductRow(row:Row):Seq[Row]={
    val cartesianProductList = Utils.cartesianProduct(
      List(
        List(row.getAs[Long](NoteFields.DATE)),
        List(row.getAs[String](NoteFields.INTERVAL)),
        List(row.getAs[String](NoteFields.BUSINESS_UNIT),"all"),
        List(row.getAs[String](NoteFields.MARKET_CENTER_ID),"all"),
        List(row.getAs[String](NoteFields.TEAM_ID),"all"),
        List(row.getAs[String](NoteFields.USER_TYPE)),
        List(row.getAs[String](NoteFields.AGENT_ID)),
        List(row.getAs[String](NoteFields.SOURCE)),
        List(row.getAs[String](NoteFields.CID),"all"),
        List(row.getAs[String](NoteFields.STATUS)),
        List(row.getAs[String](NoteFields.NOTE_ID))
      )
    )
    cartesianProductList.map(r=>Row.fromSeq(r))
  }
  def definedSchema:StructType={
    StructType(Array(
      StructField(NoteFields.DATE,LongType,true),
      StructField(NoteFields.INTERVAL,StringType,true),
      StructField(NoteFields.BUSINESS_UNIT,StringType,true),
      StructField(NoteFields.MARKET_CENTER_ID,StringType,true),
      StructField(NoteFields.TEAM_ID,StringType,true),
      StructField(NoteFields.USER_TYPE,StringType,true),
      StructField(NoteFields.AGENT_ID,StringType,true),
      StructField(NoteFields.SOURCE,StringType,true),
      StructField(NoteFields.CID,StringType,true),
      StructField(NoteFields.STATUS,StringType,true),
      StructField(NoteFields.NOTE_ID,StringType,true)
    ))
  }


}
