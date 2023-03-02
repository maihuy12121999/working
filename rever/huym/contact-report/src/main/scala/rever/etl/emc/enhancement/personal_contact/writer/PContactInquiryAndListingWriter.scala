package rever.etl.emc.enhancement.personal_contact.writer

import org.apache.spark.sql.{DataFrame, Row}
import rever.etl.rsparkflow.api.SinkWriter
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.client.PushKafkaRecord
import rever.etl.rsparkflow.domain.data_delivery.DataDeliveryMsg
import rever.etl.rsparkflow.extensions.PushKafkaWriterMixin
import rever.etl.rsparkflow.utils.JsonUtils

class PContactInquiryAndListingWriter extends SinkWriter with PushKafkaWriterMixin {

  override def write(tableName: String, df: DataFrame, config: Config): DataFrame = {

    val topic = config.get("data_delivery_personal_contact_fulfillment_topic")

    pushDataframeToKafka(config, df, topic)(toPushKafkaRecord, 800)

    df

  }

  private def toPushKafkaRecord(row: Row): PushKafkaRecord = {

    val pCid = row.getAs[String]("p_cid")
    val totalListings = row.getAs[Long]("total_listings")
    val totalInquiries = row.getAs[Long]("total_inquiries")
    val timestamp = System.currentTimeMillis()

    PushKafkaRecord(
      key = None,
      value = JsonUtils.toJson(
        DataDeliveryMsg(
          objectType = "personal_contact",
          objectId = pCid,
          replaceFields = Some(
            Map[String, Any](
              "total_rever_listing" -> totalListings,
              "total_inquiry" -> totalInquiries
            )
          ),
          appendFields = None,
          removeFields = None,
          deleteFields = None,
          timestamp = timestamp,
          dataService = "contact_health_fulfillment"
        ),
        pretty = false
      )
    )
  }
}
