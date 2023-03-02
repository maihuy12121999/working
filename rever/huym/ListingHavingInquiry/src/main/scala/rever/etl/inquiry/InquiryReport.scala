package rever.etl.inquiry
import com.fasterxml.jackson.databind.JsonNode
import org.apache.spark.sql.{DataFrame, Row, functions}
import org.apache.spark.sql.Row.empty.toSeq
import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.{col, explode, regexp_replace, when}
import rever.etl.inquiry.config.InquiryFields
import rever.etl.inquiry.reader.{InquiryReader, ListingCsvReader}
import rever.etl.inquiry.writer.InquiryWriter
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.{Output, Table}
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.client.DataMappingClient

import scala.collection.JavaConverters.asScalaBufferConverter

class InquiryReport extends FlowMixin{
  @Output(writer = classOf[InquiryWriter])
  @Table("InquiryDf")
  def build(
             @Table(
               name = "historical.inquiry_1",
               reader =classOf[InquiryReader])
             inquiryDf:DataFrame,
             @Table(
               name = "listingdf",
               reader =classOf[ListingCsvReader])
             listingDf:DataFrame,
             config : Config
           ):DataFrame={
    val enhancedInquiryDf = enhanceInquiryData(inquiryDf,config)
    enhanceListingHavingInquiry(enhancedInquiryDf,listingDf,config)
  }
  private def enhanceInquiryData(inquiryDf:DataFrame,config: Config):DataFrame={
    val resultDf = inquiryDf
      .mapPartitions {
        rows =>
          val rowSeq = rows.toSeq
          val inquiryIds = rowSeq.map(_.getAs[String](InquiryFields.INQUIRY_ID)).filterNot(_==null).filterNot(_.isEmpty).distinct
          val listingIdMap = getListingIds(inquiryIds,config)
          val ownerIds = rowSeq.map(_.getAs[String](InquiryFields.OWNER_ID)).filterNot(_==null).filterNot(_.isEmpty).distinct
          val userProfileMap = getUserProfileFromOwnerId(ownerIds,config)
          rowSeq.map {
            row =>
              val inquiryId = row.getAs[String](InquiryFields.INQUIRY_ID)
              val createdTime = row.getAs[Long](InquiryFields.CREATED_TIME)
              val updatedTime = row.getAs[Long](InquiryFields.UPDATED_TIME)
              val ownerId = row.getAs[String](InquiryFields.OWNER_ID)
              val listingIds = listingIdMap.get(inquiryId) match {
                case None => Seq.empty
                case Some(listingIds)=> listingIds
              }
              val ownerEmail = userProfileMap.get(ownerId) match {
                case None => "unknown"
                case Some(userProfile)=> userProfile._1
              }
              val ownerTeam =
                if(row.getAs[String](InquiryFields.OWNER_TEAM).isEmpty||row.getAs[String](InquiryFields.OWNER_TEAM) == null){
                  userProfileMap.get(ownerId) match {
                    case None => "unknown"
                    case Some(userProfile)=> userProfile._2
                  }
                }
                else row.getAs[String](InquiryFields.OWNER_TEAM)
              val ownerJobTitle = userProfileMap.get(ownerId) match {
                case None => "unknown"
                case Some(userProfile)=> userProfile._3
              }
              Row(inquiryId,createdTime,updatedTime,ownerId,ownerTeam,listingIds.toString)
          }.toIterator
      }(RowEncoder(InquiryHelper.inquirySchema))
    explodeListingIds(resultDf)
  }
  private def explodeListingIds(inquiryDf:DataFrame):DataFrame={
    inquiryDf
      .withColumn(
        InquiryFields.LISTING_IDS,
        regexp_replace(col(InquiryFields.LISTING_IDS), "[\\[\\'\\'\\]]", "")
      )
      .withColumn(InquiryFields.LISTING_IDS, functions.split(col(InquiryFields.LISTING_IDS), ","))
      .withColumn(InquiryFields.LISTING_ID, explode(col(InquiryFields.LISTING_IDS)))
      .drop(InquiryFields.LISTING_IDS)
  }
  private def enhanceListingHavingInquiry(inquiryDf:DataFrame,listingDf:DataFrame,config: Config):DataFrame={
    inquiryDf
      .joinWith(listingDf,inquiryDf(InquiryFields.LISTING_ID) === listingDf(InquiryFields.LISTING_ID)
        ,"right")
      .na.fill("unknown")
      .na.fill(0)
      .na.fill(0.0)
      .select(
        when(
          col(s"_1.${InquiryFields.LISTING_ID}").isNull
            .or(col(s"_1.${InquiryFields.LISTING_ID}")===""),col(s"_2.${InquiryFields.LISTING_ID}")
        )
          .otherwise(col(s"_1.${InquiryFields.LISTING_ID}")).as(InquiryFields.LISTING_ID),
        col(s"_1.${InquiryFields.INQUIRY_ID}"),
        col(s"_1.${InquiryFields.CREATED_TIME}"),
        col(s"_1.${InquiryFields.UPDATED_TIME}").as(InquiryFields.UPDATED_TIME_INQUIRY),
        when(
          col(s"_1.${InquiryFields.OWNER_ID}").isNull
            .or(col(s"_1.${InquiryFields.OWNER_ID}")===""),col(s"_2.${InquiryFields.OWNER_ID}")
        )
          .otherwise(col(s"_1.${InquiryFields.OWNER_ID}")).as(InquiryFields.OWNER_ID),
        col(s"_1.${InquiryFields.OWNER_TEAM}"),
        col(s"_2.${InquiryFields.PUBLISHED_TIME}"),
        col(s"_2.${InquiryFields.UPDATED_TIME}").as(InquiryFields.UPDATED_TIME_LISTING),
        col(s"_2.${InquiryFields.LISTING_STATUS}"),
        col(s"_2.${InquiryFields.PROPERTY_TYPE}"),
        col(s"_2.${InquiryFields.NUM_BED_ROOM}"),
        col(s"_2.${InquiryFields.SALE_PRICE}"),
        col(s"_2.${InquiryFields.AREA_USING}"),
        col(s"_2.${InquiryFields.CITY}"),
        col(s"_2.${InquiryFields.DISTRICT}"),
        col(s"_2.${InquiryFields.WARD}"),
        col(s"_2.${InquiryFields.PROJECT_NAME}")
      )
      .mapPartitions {
        rows =>
          val rowSeq = rows.toSeq
          val ownerIds = rowSeq.map(_.getAs[String](InquiryFields.OWNER_ID)).filterNot(_==null).filterNot(_.isEmpty).distinct
          val userProfileMap = getUserProfileFromOwnerId(ownerIds,config)
          rowSeq.map {
            row =>
              val listingId = row.getAs[String](InquiryFields.LISTING_ID)
              val inquiryId = row.getAs[String](InquiryFields.INQUIRY_ID)
              val createdTime = row.getAs[Long](InquiryFields.CREATED_TIME)
              val updatedTimeInquiry = row.getAs[Long](InquiryFields.UPDATED_TIME_INQUIRY)
              val ownerId = row.getAs[String](InquiryFields.OWNER_ID)
              val publishedTime = row.getAs[Long](InquiryFields.PUBLISHED_TIME)
              val updatedTimeListing = row.getAs[Long](InquiryFields.UPDATED_TIME_LISTING)
              val listingStatus = row.getAs[String](InquiryFields.LISTING_STATUS)
              val propertyType = row.getAs[String](InquiryFields.PROPERTY_TYPE)
              val numBedRoom = row.getAs[Int](InquiryFields.NUM_BED_ROOM)
              val salePrice = row.getAs[Float](InquiryFields.SALE_PRICE)
              val areaUsing = row.getAs[Float](InquiryFields.AREA_USING)
              val city = row.getAs[String](InquiryFields.CITY)
              val district = row.getAs[String](InquiryFields.DISTRICT)
              val ward = row.getAs[String](InquiryFields.WARD)
              val projectName = row.getAs[String](InquiryFields.PROJECT_NAME)

              val ownerEmail = userProfileMap.get(ownerId) match {
                case None => "unknown"
                case Some(userProfile)=> userProfile._1
              }
              val ownerTeam =
                if(row.getAs[String](InquiryFields.OWNER_TEAM).isEmpty||row.getAs[String](InquiryFields.OWNER_TEAM) == null){
                  userProfileMap.get(ownerId) match {
                    case None => "unknown"
                    case Some(userProfile)=> userProfile._2
                  }
                }
                else row.getAs[String](InquiryFields.OWNER_TEAM)
              val ownerJobTitle = userProfileMap.get(ownerId) match {
                case None => "unknown"
                case Some(userProfile)=> userProfile._3
              }
              val ownerDepartment = ""
              Row(listingId,inquiryId,createdTime,updatedTimeInquiry,ownerId,ownerTeam,ownerEmail,ownerJobTitle
                ,ownerDepartment,publishedTime,updatedTimeListing,listingStatus,propertyType,numBedRoom,salePrice
                ,areaUsing,city,district,ward,projectName)
          }.toIterator
      }(RowEncoder(InquiryHelper.listingInquirySchema))





      .na.fill("unknown")
//      .mapPartitions{
//        rows=>
//          val rowSeq = rows.toSeq
//          val ownerIds = rowSeq.map(_.getAs[String](InquiryFields.OWNER_ID)).filterNot(_==null).filterNot(_.isEmpty).distinct
//          val
//      }


  }
  private def getUserProfileFromOwnerId(ownerIds: Seq[String], config: Config): Map[String, (String,String,String)] = {
    val client = DataMappingClient.client(config)
    val userIdMap = client.mGetUser(ownerIds)
    userIdMap.map(
      e=>
        e._1->
          (
            e._2.workEmail("unknown"),
            e._2.teamId("unknown"),
            e._2.userNode.at("/properties/job_title").asText("unknown")
          )
    )
  }
  private def getListingIds(inquiryIds:Seq[String],config: Config):Map[String,Seq[String]]={
    val client = DataMappingClient.client(config)
//    val getListingIdMap = client.mGetListingId(inquiryIds)
//    getListingIdMap
    Map.empty
  }
  private def toDepartment(teamId:String,config: Config):String ={
    val teachTeamIds = config.getList("tech_team_ids",",").asScala.toSet
    teamId match {
      case ""=>"unknown"
      case teamId =>
        if(teachTeamIds.contains(teamId)) "tech"
        else "non_tech"
    }
  }
}
