package rever.etl.data_sync.normalizer

import com.fasterxml.jackson.databind.JsonNode
import org.elasticsearch.search.SearchHit
import rever.etl.data_sync.core.Normalizer
import rever.etl.rsparkflow.utils.JsonUtils

/** @author anhlt (andy)
  */
case class TrackingDataNormalizer() extends Normalizer[SearchHit, Map[String, Any]] {

  override def toRecord(searchHit: SearchHit, total: Long): Option[Map[String, Any]] = {
    val jsonDoc = JsonUtils.fromJson[JsonNode](searchHit.getSourceAsString)

    val historyId = searchHit.getId
    val objectType = searchHit.getType
    val objectId = jsonDoc.at("/id").asText("")
    val action = jsonDoc.at("/action").asText("")
    val result = jsonDoc.at("/result").asText("")

    val oldData = Some(jsonDoc.at("/old_data"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .map {
        case node if node.isContainerNode => node
        case node                         => JsonUtils.toJsonNode(node.asText("{}"))
      }
      .getOrElse(JsonUtils.toJsonNode("{}"))

    val newData = Some(jsonDoc.at("/new_data"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .map {
        case node if node.isContainerNode => node
        case node                         => JsonUtils.toJsonNode(node.asText("{}"))
      }
      .getOrElse(JsonUtils.toJsonNode("{}"))

    val performer = Some(jsonDoc.at("/user"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .map {
        case node if node.isContainerNode => node
        case node                         => JsonUtils.toJsonNode(node.asText("{}"))
      }
      .getOrElse(JsonUtils.toJsonNode("{}"))

    val timestamp = jsonDoc.at("/timestamp").asLong(0)

    val dataMap = Map(
      "es_history_id" -> historyId,
      "object_id" -> objectId,
      "action" -> action,
      "data" -> newData,
      "old_data" -> oldData,
      "source" -> "",
      "performer" -> performer,
      "timestamp" -> timestamp,
      "log_time" -> System.currentTimeMillis()
    )

    Some(dataMap)
  }
}
