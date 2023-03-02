package rever.etl.listing.view_impressions.writer

import com.fasterxml.jackson.databind
import org.apache.spark.sql.functions.lit
import org.apache.spark.sql.{DataFrame, Row}
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.extensions.RapIngestWriterMixin
import rever.etl.rsparkflow.utils.JsonUtils

class ListingImpressionWriter extends SinkWriter with RapIngestWriterMixin {
  override def write(tableName: String, dataFrame: DataFrame, config: Config): DataFrame = {

    val topic = config.get("listing_view_impressions_topic")
    val df = dataFrame.withColumn("date", lit(config.getDailyReportTime))
    ingestDataframe(config, df, topic)(toListingViewRecord)

    mergeIfRequired(config, df, topic)

    df
  }

  private def toListingViewRecord(row: Row): databind.JsonNode = {
    val jsonRow = JsonUtils.toJson(
      Map(
        "date" -> row.getAs[Long]("date"),
        "domain" -> row.getAs[String]("domain"),
        "property_id" -> row.getAs[String]("property_id"),
        "total_a1_viewers" -> row.getAs[Long]("total_a1_viewers"),
        "total_a1_users" -> row.getAs[Long]("total_a1_users"),
        "a1" -> row.getAs[Long]("a1"),
        "a7" -> row.getAs[Long]("a7"),
        "a30" -> row.getAs[Long]("a30"),
        "a0" -> row.getAs[Long]("a0")
      )
    )
    JsonUtils.toJsonNode(jsonRow)
  }
}
