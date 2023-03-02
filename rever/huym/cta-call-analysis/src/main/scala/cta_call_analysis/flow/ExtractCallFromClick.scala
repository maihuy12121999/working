package cta_call_analysis.flow

import com.fasterxml.jackson.databind.JsonNode
import cta_call_analysis.writer.TestExtractCallFromCLickWriter
import org.apache.spark.SparkContext
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.broadcast
import org.apache.spark.sql.types.StructType
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.{Output, Table}
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.client.DataMappingClient

import scala.collection.mutable.ListBuffer
import scala.language.postfixOps
import scala.util.control.Breaks.{break, breakable}
class ExtractCallFromClick extends FlowMixin{
  @Output(writer = classOf[TestExtractCallFromCLickWriter])
  @Table("ExtractCallFromClickDF")
  def build(
           @Table(name = "UserEventData") userEventDF:DataFrame,
           @Table(name = "CallHistoryData") callHistoryDF:DataFrame,
           config: Config
           ):DataFrame={
    val schema:StructType = StructType(userEventDF.schema ++ callHistoryDF.schema)
    val callHistorySortedBroadcast =SparkSession
      .active.sparkContext
      .broadcast(callHistoryDF.sort("start_time").collect())
    userEventDF.map(ctaToCallHistoryRow(_,callHistorySortedBroadcast,config))(RowEncoder(schema))
  }
  def ctaToCallHistoryRow(row : Row,callHistorySortedBroadcast:Broadcast[Array[Row]], config: Config): Row ={
      val durationLimitBetweenNonLoggedUserClickAndCall = config
        .get("DURATION_LIMIT_BETWEEN_NON_LOGGED_USER_CLICK_AND_CALL").toLong
      val durationLimitBetweenLoggedUserClickAndCall = config
        .get("DURATION_LIMIT_BETWEEN_LOGGED_USER_CLICK_AND_CALL").toLong
      val url = row.getString(0)
      val clickTimeStamp = row.getLong(1)
      val userId = row.getString(2)
      val sourceType = row.getString(3)
      val alias = row.getString(4)
      val sourceCode = row.getString(5)
      val phoneNumber = row.getString(6)
      val ctaCallHistoryMatch =  callHistorySortedBroadcast.value.filter(callRow => {
        val sourcePhone = callRow.getString(0)
//        val sourcePhone = "+" + phoneNumber
        val transformedSourcePhone =
          if (sourcePhone!="" && sourcePhone.head == '+') sourcePhone.substring(1) else sourcePhone
        val callStartTime = callRow.getLong(5)
        //        val callStartTime = clickTimeStamp + durationLimitBetweenLoggedUserClickAndCall
        if (phoneNumber == transformedSourcePhone
          && callStartTime >= clickTimeStamp
          && callStartTime <= clickTimeStamp + durationLimitBetweenLoggedUserClickAndCall) {
            true
        }
        else if (callStartTime >= clickTimeStamp
          && callStartTime <= clickTimeStamp + durationLimitBetweenNonLoggedUserClickAndCall) {
            true
        }
        else{false}
      }).sortBy(row=>(row.getString(0)!=phoneNumber, row.getLong(5)))
      ctaCallHistoryMatch.headOption match {
        case Some(row)=>
            val sourcePhone = row.getString(0)
            val destination = row.getString(1)
            val extension = row.getString(2)
            val status = row.getString(3)
            val duration = row.getLong(4)
            val callStartTime = row.getLong(5)
            val endTime = row.getLong(6)
            Row(url, clickTimeStamp, userId, sourceType, alias, sourceCode, phoneNumber
              , sourcePhone, destination, extension, status, duration, callStartTime, endTime)
        case None =>
          Row(url, clickTimeStamp, userId, sourceType, alias, sourceCode, phoneNumber, "", "", "", "", null, null, null)
      }

  }
}
