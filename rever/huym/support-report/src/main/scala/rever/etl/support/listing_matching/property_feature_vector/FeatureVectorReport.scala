package rever.etl.support.listing_matching.property_feature_vector

import org.apache.spark.sql.DataFrame
import rever.rsparkflow.spark.FlowMixin
import rever.rsparkflow.spark.api.annotation.{Output, Table}
import rever.rsparkflow.spark.api.configuration.Config
import rever.etl.support.listing_matching.property_feature_vector.reader.{CsvListingDMReader, ListingDMReader}
import rever.etl.support.listing_matching.property_feature_vector.writer.FeatureVectorWriter

class FeatureVectorReport extends FlowMixin {
  @Output(writer = classOf[FeatureVectorWriter])
  @Table("feature_vector")
  def build(
      @Table(name = "listing_df", reader = classOf[CsvListingDMReader]) listingDf: DataFrame,
      config: Config
  ): DataFrame = {
    listingDf.na
      .fill("")
      .na
      .fill(0.0)
  }

}
