package rever.etl.engagement.linquiry

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions._
import org.apache.spark.sql.{DataFrame, Row, functions}
import rever.etl.engagement.linquiry.config.ListingInquiryFields
import rever.etl.engagement.linquiry.reader.{InquiryReader, ListingReader}
import rever.etl.engagement.linquiry.writer.ListingInquiryWriter
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.{Output, Table}
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.client.DataMappingClient

class ListingInquiryReport extends FlowMixin {
  @Output(writer = classOf[ListingInquiryWriter])
  @Table("listing_inquiry_df")
  def build(
      @Table(name = "historical.inquiry_1", reader = classOf[InquiryReader])
      inquiryDf: DataFrame,
      @Table(name = "listing_df", reader = classOf[ListingReader])
      listingDf: DataFrame,
      config: Config
  ): DataFrame = {
    val enhancedInquiryDf = enhanceInquiryData(inquiryDf, config)
    val enhanceListingDf = enhanceListingData(listingDf, config)

    normalizeData(enhancedInquiryDf, enhanceListingDf, config)
      .groupBy(
        col(ListingInquiryFields.LISTING_ID),
        col(ListingInquiryFields.PUBLISHED_TIME),
        col(ListingInquiryFields.UPDATED_TIME_LISTING),
        col(ListingInquiryFields.LISTING_STATUS),
        col(ListingInquiryFields.PROPERTY_TYPE),
        col(ListingInquiryFields.NUM_BED_ROOM),
        col(ListingInquiryFields.SALE_PRICE),
        col(ListingInquiryFields.AREA_USING),
        col(ListingInquiryFields.CITY),
        col(ListingInquiryFields.DISTRICT),
        col(ListingInquiryFields.WARD),
        col(ListingInquiryFields.PROJECT_NAME),
        col(ListingInquiryFields.LISTING_OWNER_ID),
        col(ListingInquiryFields.LISTING_OWNER_EMAIL),
        col(ListingInquiryFields.LISTING_OWNER_JOB_TITLE),
        col(ListingInquiryFields.LISTING_OWNER_TEAM),
        col(ListingInquiryFields.LISTING_OWNER_DEPARTMENT),
        col(ListingInquiryFields.INQUIRY_ID),
        col(ListingInquiryFields.CREATED_TIME),
        col(ListingInquiryFields.UPDATED_TIME_INQUIRY),
        col(ListingInquiryFields.INQUIRY_OWNER_ID),
        col(ListingInquiryFields.INQUIRY_OWNER_EMAIL),
        col(ListingInquiryFields.INQUIRY_OWNER_JOB_TITLE),
        col(ListingInquiryFields.INQUIRY_OWNER_TEAM),
        col(ListingInquiryFields.INQUIRY_OWNER_DEPARTMENT)
      )
      .agg(
        countDistinct(col(ListingInquiryFields.LISTING_ID)).as("total")
      )
      .drop("total")
  }
  private def enhanceInquiryData(inquiryDf: DataFrame, config: Config): DataFrame = {
    val resultDf = inquiryDf
      .mapPartitions { rows =>
        val rowSeq = rows.toSeq
        val ownerIds = rowSeq
          .map(_.getAs[String](ListingInquiryFields.OWNER_ID))
          .filterNot(_ == null)
          .filterNot(_.isEmpty)
          .distinct

        val inquiryIds = rowSeq
          .map(_.getAs[String](ListingInquiryFields.INQUIRY_ID))
          .filterNot(_ == null)
          .filterNot(_.isEmpty)
          .distinct

        val listingIdMap = getListingIds(inquiryIds, config)

        val userProfileMap = getUserProfileFromOwnerId(ownerIds, config)

        rowSeq.map { row =>
          val inquiryId = row.getAs[String](ListingInquiryFields.INQUIRY_ID)
          val createdTime = row.getAs[Long](ListingInquiryFields.CREATED_TIME)
          val updatedTime = row.getAs[Long](ListingInquiryFields.UPDATED_TIME)
          val ownerId = row.getAs[String](ListingInquiryFields.OWNER_ID)
          var ownerTeam = row.getAs[String](ListingInquiryFields.OWNER_TEAM)

          val ownerEmail = userProfileMap.get(ownerId) match {
            case None              => "unknown"
            case Some(userProfile) => userProfile._1
          }

          ownerTeam = ownerTeam match {
            case "" | null =>
              userProfileMap.get(ownerId) match {
                case None              => "unknown"
                case Some(userProfile) => userProfile._2
              }
            case _ => ownerTeam
          }

          val ownerJobTitle = userProfileMap.get(ownerId) match {
            case None              => "unknown"
            case Some(userProfile) => userProfile._3
          }

          val listingIds = listingIdMap.get(inquiryId) match {
            case None             => Seq.empty
            case Some(listingIds) => listingIds
          }

          val ownerDepartment = getDepartment()

          Row(
            inquiryId,
            createdTime,
            updatedTime,
            ownerId,
            ownerTeam,
            ownerEmail,
            ownerJobTitle,
            ownerDepartment,
            listingIds.toString
          )
        }.toIterator
      }(RowEncoder(ListingInquiryHelper.inquirySchema))

    explodeListingIds(resultDf)
  }
  private def explodeListingIds(inquiryDf: DataFrame): DataFrame = {
    inquiryDf
      .withColumn(
        ListingInquiryFields.LISTING_IDS,
        regexp_replace(col(ListingInquiryFields.LISTING_IDS), "[\\[\\'\\'\\]]", "")
      )
      .withColumn(ListingInquiryFields.LISTING_IDS, functions.split(col(ListingInquiryFields.LISTING_IDS), ","))
      .withColumn(ListingInquiryFields.LISTING_ID, explode(col(ListingInquiryFields.LISTING_IDS)))
      .drop(ListingInquiryFields.LISTING_IDS)
  }

  private def enhanceListingData(listingDf: DataFrame, config: Config): DataFrame = {
    listingDf
      .mapPartitions { rows =>
        val rowSeq = rows.toSeq
        val ownerIds = rowSeq
          .map(_.getAs[String](ListingInquiryFields.OWNER_ID))
          .filterNot(_ == null)
          .filterNot(_.isEmpty)
          .distinct

        val userProfileMap = getUserProfileFromOwnerId(ownerIds, config)

        rowSeq.map { row =>
          val listingId = row.getAs[String](ListingInquiryFields.LISTING_ID)
          val publishedTime = row.getAs[Long](ListingInquiryFields.PUBLISHED_TIME)
          val updatedTime = row.getAs[Long](ListingInquiryFields.UPDATED_TIME)
          val listingStatus = row.getAs[String](ListingInquiryFields.LISTING_STATUS)
          val propertyType = row.getAs[String](ListingInquiryFields.PROPERTY_TYPE)
          val numBedRoom = row.getAs[Integer](ListingInquiryFields.NUM_BED_ROOM)
          val salePrice = row.getAs[Double](ListingInquiryFields.SALE_PRICE)
          val areaUsing = row.getAs[Double](ListingInquiryFields.AREA_USING)
          val city = row.getAs[String](ListingInquiryFields.CITY)
          val district = row.getAs[String](ListingInquiryFields.DISTRICT)
          val ward = row.getAs[String](ListingInquiryFields.WARD)
          val projectName = row.getAs[String](ListingInquiryFields.PROJECT_NAME)
          val ownerId = row.getAs[String](ListingInquiryFields.OWNER_ID)

          val ownerEmail = userProfileMap.get(ownerId) match {
            case None              => "unknown"
            case Some(userProfile) => userProfile._1
          }

          val ownerTeam = userProfileMap.get(ownerId) match {
            case None              => "unknown"
            case Some(userProfile) => userProfile._2
          }

          val ownerJobTitle = userProfileMap.get(ownerId) match {
            case None              => "unknown"
            case Some(userProfile) => userProfile._3
          }

          val ownerDepartment = getDepartment()

          Row(
            listingId,
            publishedTime,
            updatedTime,
            listingStatus,
            propertyType,
            numBedRoom,
            salePrice,
            areaUsing,
            city,
            district,
            ward,
            projectName,
            ownerId,
            ownerTeam,
            ownerEmail,
            ownerJobTitle,
            ownerDepartment
          )
        }.toIterator
      }(RowEncoder(ListingInquiryHelper.listingSchema))
  }
  private def normalizeData(inquiryDf: DataFrame, listingDf: DataFrame, config: Config): DataFrame = {
    inquiryDf
      .joinWith(
        listingDf,
        inquiryDf(ListingInquiryFields.LISTING_ID) === listingDf(ListingInquiryFields.LISTING_ID),
        "right"
      )
      .select(
        when(
          col(s"_1.${ListingInquiryFields.LISTING_ID}").isNull
            .or(col(s"_1.${ListingInquiryFields.LISTING_ID}") === ""),
          col(s"_2.${ListingInquiryFields.LISTING_ID}")
        )
          .otherwise(col(s"_1.${ListingInquiryFields.LISTING_ID}"))
          .as(ListingInquiryFields.LISTING_ID),
        col(s"_1.${ListingInquiryFields.INQUIRY_ID}"),
        col(s"_1.${ListingInquiryFields.CREATED_TIME}"),
        col(s"_1.${ListingInquiryFields.UPDATED_TIME}").as(ListingInquiryFields.UPDATED_TIME_INQUIRY),
        col(s"_1.${ListingInquiryFields.OWNER_TEAM}").as(ListingInquiryFields.INQUIRY_OWNER_TEAM),
        col(s"_1.${ListingInquiryFields.OWNER_ID}").as(ListingInquiryFields.INQUIRY_OWNER_ID),
        col(s"_2.${ListingInquiryFields.OWNER_ID}").as(ListingInquiryFields.LISTING_OWNER_ID),
        col(s"_2.${ListingInquiryFields.OWNER_TEAM}").as(ListingInquiryFields.LISTING_OWNER_TEAM),
        col(s"_1.${ListingInquiryFields.OWNER_EMAIL}").as(ListingInquiryFields.INQUIRY_OWNER_EMAIL),
        col(s"_2.${ListingInquiryFields.OWNER_EMAIL}").as(ListingInquiryFields.LISTING_OWNER_EMAIL),
        col(s"_1.${ListingInquiryFields.OWNER_JOB_TITLE}").as(ListingInquiryFields.INQUIRY_OWNER_JOB_TITLE),
        col(s"_2.${ListingInquiryFields.OWNER_JOB_TITLE}").as(ListingInquiryFields.LISTING_OWNER_JOB_TITLE),
        col(s"_1.${ListingInquiryFields.OWNER_DEPARTMENT}").as(ListingInquiryFields.INQUIRY_OWNER_DEPARTMENT),
        col(s"_2.${ListingInquiryFields.OWNER_DEPARTMENT}").as(ListingInquiryFields.LISTING_OWNER_DEPARTMENT),
        col(s"_2.${ListingInquiryFields.PUBLISHED_TIME}"),
        col(s"_2.${ListingInquiryFields.UPDATED_TIME}").as(ListingInquiryFields.UPDATED_TIME_LISTING),
        col(s"_2.${ListingInquiryFields.LISTING_STATUS}"),
        col(s"_2.${ListingInquiryFields.PROPERTY_TYPE}"),
        col(s"_2.${ListingInquiryFields.NUM_BED_ROOM}"),
        col(s"_2.${ListingInquiryFields.SALE_PRICE}"),
        col(s"_2.${ListingInquiryFields.AREA_USING}"),
        col(s"_2.${ListingInquiryFields.CITY}"),
        col(s"_2.${ListingInquiryFields.DISTRICT}"),
        col(s"_2.${ListingInquiryFields.WARD}"),
        col(s"_2.${ListingInquiryFields.PROJECT_NAME}")
      )
      .na
      .fill(0)
      .na
      .fill(0.0)
      .na
      .fill(0L)
      .na
      .fill("unknown")
      .na
      .replace(
        Seq(
          ListingInquiryFields.LISTING_STATUS,
          ListingInquiryFields.PROPERTY_TYPE,
          ListingInquiryFields.CITY,
          ListingInquiryFields.DISTRICT,
          ListingInquiryFields.WARD,
          ListingInquiryFields.PROJECT_NAME,
          ListingInquiryFields.LISTING_OWNER_ID,
          ListingInquiryFields.LISTING_OWNER_EMAIL,
          ListingInquiryFields.LISTING_OWNER_JOB_TITLE,
          ListingInquiryFields.LISTING_OWNER_TEAM,
          ListingInquiryFields.LISTING_OWNER_DEPARTMENT,
          ListingInquiryFields.INQUIRY_OWNER_ID,
          ListingInquiryFields.INQUIRY_OWNER_EMAIL,
          ListingInquiryFields.INQUIRY_OWNER_JOB_TITLE,
          ListingInquiryFields.INQUIRY_OWNER_TEAM,
          ListingInquiryFields.INQUIRY_OWNER_DEPARTMENT
        ),
        Map("" -> "unknown")
      )

  }
  private def getUserProfileFromOwnerId(
      ownerIds: Seq[String],
      config: Config
  ): Map[String, (String, String, String)] = {
    val client = DataMappingClient.client(config)
    val userIdMap = client.mGetUser(ownerIds)
    userIdMap.map(e =>
      e._1 ->
        (
          e._2.workEmail("unknown"),
          e._2.teamId("unknown"),
          e._2.userNode.at("/properties/job_title").asText("unknown")
        )
    )
  }


  private def getListingIds(inquiryIds: Seq[String], config: Config): Map[String, Seq[String]] = {
    val client = DataMappingClient.client(config)

    val resultMap = scala.collection.mutable.Map.empty[String, Seq[String]]
    inquiryIds
      .grouped(50)
      .map(client.mGetInquiryPropertyIds)
      .foreach(map => {
        map.foreach(e => resultMap.put(e._1, e._2))
      })

    resultMap.toMap
  }

  private def getDepartment(): String = {
//    val teachTeamIds = config.getList("tech_team_ids",",").asScala.toSet
//    teamId match {
//      case ""=>"unknown"
//      case teamId =>
//        if(teachTeamIds.contains(teamId)) "tech"
//        else "non_tech"
//    }
    ""
  }
}
