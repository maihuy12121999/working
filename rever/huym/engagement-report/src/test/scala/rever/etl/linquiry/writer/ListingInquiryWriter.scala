package rever.etl.engagement.linquiry.writer

import org.apache.spark.sql.{DataFrame, SaveMode}
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config

class ListingInquiryWriter extends SinkWriter {
  override def write(tableName: String, dataFrame: DataFrame, config: Config): DataFrame = {

    dataFrame.write
      .mode(SaveMode.Overwrite)
      .options(Map("delimiter" -> ",", "header" -> "true", "inferSchema" -> "true"))
      .format("csv")
      .save("output/listing_inquiry.csv")

    dataFrame
  }
}
