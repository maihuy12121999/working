package rever.etl.listing.published_listings

import org.apache.spark.sql.Row
import rever.etl.listing.domain.PublishedListingFields
import rever.etl.rsparkflow.utils.Utils

object PublishedListingSchema {

  def generateRow(row: Row): Seq[Row] = {

    val cartesianRows = Utils.cartesianProduct(
      List(
        List(row.getAs[Long](PublishedListingFields.DATE)),
        List(row.getAs[String](PublishedListingFields.LISTING_CHANNEL)),
        List(row.getAs[String](PublishedListingFields.PUBLISH_TYPE)),
        List(row.getAs[String](PublishedListingFields.SERVICE_TYPE), "all"),
        List(row.getAs[String](PublishedListingFields.BUSINESS_UNIT), "all"),
        List(row.getAs[String](PublishedListingFields.MARKET_CENTER_ID), "all"),
        List(row.getAs[String](PublishedListingFields.TEAM_ID), "all"),
        List(row.getAs[String](PublishedListingFields.CITY_ID), "all"),
        List(row.getAs[String](PublishedListingFields.DISTRICT_ID), "all"),
        List(row.getAs[String](PublishedListingFields.PROJECT_ID)),
        List(row.getAs[String](PublishedListingFields.AGENT_ID)),
        List(row.getAs[String](PublishedListingFields.USER_TYPE)),
        List(row.getAs[String](PublishedListingFields.PUBLISHER_ID)),
        List(row.getAs[String](PublishedListingFields.CID), "all"),
        List(row.getAs[String](PublishedListingFields.PROPERTY_ID))
      )
    )
    cartesianRows.flatMap(r => Seq(Row.fromSeq(r)))
  }
}
