package cta_call_analysis.flow
import com.fasterxml.jackson.databind.JsonNode
import cta_call_analysis.reader.UserEventReader
import cta_call_analysis.writer.TestUserEventWriter
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.{DataFrame, Row}
import org.apache.spark.sql.functions.{col, lit, udf, when}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.{Output, Table}
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.client.DataMappingClient

import java.net.URL
import scala.collection.mutable.ListBuffer
import scala.language.postfixOps
import scala.util.control.Breaks.{break, breakable}
object UserEventStatistics{
  val SOURCE_TYPE_LISTING = "Listing"
  val SOURCE_TYPE_PROJECT = "Project"
  val SOURCE_TYPE_PROFILE = "Profile"
  val SOURCE_TYPE_UNKNOWN = ""
}
class UserEventStatistics extends FlowMixin{
  @Output(writer = classOf[TestUserEventWriter])
  @Table("UserEventData")
  def build(
           @Table(
             name = "segment_tracking.raw_data_normalized",
             reader =classOf[UserEventReader])
           userEventData:DataFrame,
           config : Config
           ):DataFrame={
    val getAliasFromUrlUdf = udf((url:String) => getAliasFromUrl(url))
    val schema = StructType(Array(
      StructField("url",StringType,true),
      StructField("timestamp",LongType,true),
      StructField("user_id",StringType,true),
      StructField("source_type",StringType,true),
      StructField("alias",StringType,true),
      StructField("source_code",StringType,true),
      StructField("phone_number",StringType,true),
    ))
    val addSourceCodeAndPhoneNumberDF = userEventData
      .withColumn("timestamp", col("timestamp").cast(LongType))
      .withColumn(
    "source_type",
      when(
        col("url").contains("rever.vn/mua/"),UserEventStatistics.SOURCE_TYPE_LISTING      )
      .when(
        col("url").contains("rever.vn/du-an/"),UserEventStatistics.SOURCE_TYPE_PROJECT
      )
      .when(
        col("url").contains("rever.vn/chuyen-vien/"),UserEventStatistics.SOURCE_TYPE_PROFILE
      )
      .otherwise(UserEventStatistics.SOURCE_TYPE_UNKNOWN)
    )
      .withColumn("alias", getAliasFromUrlUdf(col("url")))
      .mapPartitions(rows=>
      {
        val rowSeq :Seq[Row] = rows.toSeq
//        val mapUserId = getNumberPhoneFromUserId(Seq(userId), config)
        val listingProjectAliases = rowSeq.filterNot(_.getString(3)==UserEventStatistics.SOURCE_TYPE_PROFILE).map(_.getString(4))
        val userIds = rowSeq.map(_.getString(2)).filter(_!= null).filter(_!="")
        val userIdToPhoneNumberMap = getNumberPhoneFromUserId(userIds, config)
        val listingProjectAliasToIdMap = getIdFromAlias(listingProjectAliases,config)
        rowSeq.map(row => {
            val url = row.getString(0)
            val timeStamp = row.getLong(1)
            val userId = row.getString(2)
            val sourceType = row.getString(3)
            val alias = row.getString(4)
            val phoneNumber = userIdToPhoneNumberMap.getOrElse(userId,"")
            sourceType match {
              case UserEventStatistics.SOURCE_TYPE_LISTING | UserEventStatistics.SOURCE_TYPE_PROJECT =>
                Row(url,timeStamp,userId,sourceType,alias, listingProjectAliasToIdMap.getOrElse(alias,""),phoneNumber)
              case UserEventStatistics.SOURCE_TYPE_PROFILE =>
                Row(url,timeStamp,userId,sourceType,alias,alias + "@rever.vn",phoneNumber)
              case _ =>
                Row(url,timeStamp,userId,sourceType,alias,"",phoneNumber)
            }
          }
        ).toIterator
      })(RowEncoder(schema))
    addSourceCodeAndPhoneNumberDF
  }
  def getAliasFromUrl(url:String): String={
    val sourceTypeSet = Set("mua", "du-an", "chuyen-vien")
    try {
      url match {
        case null | "" => ""
        case _ =>
          val myUrl = new URL(url)
          val path = myUrl.getPath
          val pathNames = path.split("/")

          if (myUrl.getHost.endsWith("rever.vn") & sourceTypeSet.contains(pathNames(1)))
            pathNames(2).trim
          else ""
      }
    }
    catch{
      case e: Exception => ""
    }
  }
  def getIdFromAlias(aliases: Seq[String], config: Config): Map[String, String]= {
    val client = DataMappingClient.client(config)
    val mapId=client.mGetIdFromAlias(aliases)
    mapId
  }
  def getNumberPhoneFromUserId(userIds: Seq[String], config: Config): Map[String, String] = {
    val client = DataMappingClient.client(config)
    val userIdMap = client.mGetFrontendUserProfiles(userIds)
    userIdMap.map{ case (userId, userProfileJson) =>
      val phoneNumber = userProfileJson.at("/phone_number").asText("")
      userId -> phoneNumber
    }
  }
}
