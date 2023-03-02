package rever.etl.listing

import org.apache.spark.sql.catalyst.encoders.RowEncoder
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.{DataFrame, Row}
import rever.etl.listing.config.UpdatedListingField
import rever.etl.listing.reader.{ListingReader, UpdatedListingReader}
import rever.etl.listing.writer.UpdatedListingWriter
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.{Output, Table}
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.client.DataMappingClient

class UpdatedListingReport extends FlowMixin {
  @Output(writer = classOf[UpdatedListingWriter])
  @Table("updated_listing")
  def build(
      @Table(
        name = "historical.engagement_historical_1",
        reader = classOf[ListingReader]
      ) listingDf: DataFrame,
      @Table(
        name = "current_a0_df",
        reader = classOf[UpdatedListingReader]
      ) listingHistoricalDf: DataFrame,
      config: Config
  ): DataFrame = {
    val enhancedUpdatedListingDf = enhanceUpdatedListingData(listingHistoricalDf, config)
    val resultDf = enhanceUpdatedListingReportData(enhancedUpdatedListingDf, listingDf)
    resultDf.printSchema()
    resultDf
      .withColumn(
        UpdatedListingField.EDITED_FIELDS_INFO,
        UpdatedListingHelper.editedInfoFieldsUdf(col(UpdatedListingField.DATA), col(UpdatedListingField.OLD_DATA))
      )
      .drop(UpdatedListingField.DATA, UpdatedListingField.OLD_DATA)
  }

  private def enhanceUpdatedListingData(updatedListingDf: DataFrame, config: Config): DataFrame = {
    updatedListingDf.mapPartitions { rows =>
      val rowSeq = rows.toSeq
      val ownerIds = rowSeq
        .map(_.getAs[String](UpdatedListingField.OWNER_ID))
        .filterNot(_ == null)
        .filterNot(_.isEmpty)
        .distinct
      val profileOwnerMap = getOwnerEmailFromOwnerId(ownerIds, config)
      rowSeq.map { row =>
        val ownerId = row.getAs[String](UpdatedListingField.OWNER_ID)
        val reverId = row.getAs[String](UpdatedListingField.REVER_ID)
        val updatedTime = row.getAs[Long](UpdatedListingField.UPDATED_TIME)
        val data = row.getAs[String](UpdatedListingField.DATA)
        val oldData = row.getAs[String](UpdatedListingField.OLD_DATA)
        val listingId = row.getAs[String](UpdatedListingField.LISTING_ID)
        val ownerEmail = profileOwnerMap.getOrElse(ownerId, "unknown")
        Row(ownerEmail, reverId, updatedTime, data, oldData, listingId)
      }.toIterator
    }(RowEncoder(UpdatedListingHelper.enhanceUpdatedListingSchema))
  }

  private def getOwnerEmailFromOwnerId(ownerIds: Seq[String], config: Config): Map[String, String] = {
    val client = DataMappingClient.client(config)
    val profileOwnerMap = client.mGetUser(ownerIds)
    profileOwnerMap.map(e => e._1 -> e._2.workEmail("unknown"))
  }

  private def enhanceUpdatedListingReportData(updatedListingDf: DataFrame, listingDf: DataFrame): DataFrame = {
    updatedListingDf
      .joinWith(
        listingDf,
        updatedListingDf(UpdatedListingField.LISTING_ID) === listingDf(UpdatedListingField.LISTING_ID),
        "left"
      )
      .select(
        col(s"_1.${UpdatedListingField.OWNER_EMAIL}"),
        col(s"_1.${UpdatedListingField.REVER_ID}"),
        col(s"_1.${UpdatedListingField.UPDATED_TIME}"),
        col(s"_1.${UpdatedListingField.DATA}"),
        col(s"_1.${UpdatedListingField.OLD_DATA}"),
        col(s"_2.${UpdatedListingField.LISTING_STATUS}"),
        col(s"_2.${UpdatedListingField.UPDATED_STATUS_TIME}")
      )
  }
}
