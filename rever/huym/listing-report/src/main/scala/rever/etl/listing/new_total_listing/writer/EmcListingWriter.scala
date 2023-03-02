package rever.etl.listing.new_total_listing.writer

import org.apache.spark.sql.DataFrame
import rever.etl.listing.new_total_listing.EmcListingHelper
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.extensions.RapIngestWriterMixin

class EmcListingWriter extends SinkWriter with RapIngestWriterMixin {
  override def write(tableName: String, dataFrame: DataFrame, config: Config): DataFrame = {

    val topic = config.get("total_listing_topic")

    ingestDataframe(config, dataFrame, topic)(EmcListingHelper.toTotalListingRecord, 800)

    mergeIfRequired(config, dataFrame, topic)

    dataFrame

  }
}
