package rever.etl.emc.enhancement.personal_contact

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.{col, sum}
import org.apache.spark.sql.types.{DataType, DataTypes}
import rever.etl.emc.enhancement.personal_contact.writer.PContactInquiryAndListingWriter
import rever.etl.rsparkflow.FlowMixin
import rever.etl.rsparkflow.api.annotation.{Output, Table}
import rever.etl.rsparkflow.api.configuration.Config

/** @author anhlt
  */
class PersonalContactEnrichment extends FlowMixin {

  @Output(writer = classOf[PContactInquiryAndListingWriter])
  @Table(name = "personal_contact_inquiry_and_listing")
  def build(
      @Table(
        name = "raw_p_contact_inquiry",
        reader = classOf[reader.PContactInquiryReader]
      ) pContactInquiryDf: DataFrame,
      @Table(
        name = "raw_p_contact_listing",
        reader = classOf[reader.PContactListingReader]
      ) pContactListingDf: DataFrame,
      config: Config
  ): DataFrame = {

    val df = pContactInquiryDf
      .unionByName(pContactListingDf)
      .groupBy(
        col("p_cid")
      )
      .agg(
        sum(col("total_inquiries")).cast(DataTypes.LongType).as("total_inquiries"),
        sum(col("total_listings")).cast(DataTypes.LongType).as("total_listings")
      )
    df

  }

}
