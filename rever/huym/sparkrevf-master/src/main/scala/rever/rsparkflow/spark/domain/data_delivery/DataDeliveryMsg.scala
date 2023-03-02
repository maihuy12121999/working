package rever.rsparkflow.spark.domain.data_delivery

case class DataDeliveryMsg(
    objectType: String,
    objectId: String,
    replaceFields: Option[Map[String, Any]],
    appendFields: Option[Map[String, Any]],
    removeFields: Option[Map[String, Any]],
    deleteFields: Option[Seq[String]],
    timestamp: Long,
    dataService: String
)
