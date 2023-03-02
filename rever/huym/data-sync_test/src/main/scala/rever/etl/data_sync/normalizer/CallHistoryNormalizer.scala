package rever.etl.data_sync.normalizer

import com.fasterxml.jackson.databind.JsonNode
import org.elasticsearch.search.SearchHit
import rever.etl.data_sync.core.Normalizer
import rever.etl.rsparkflow.utils.JsonUtils

/** @author anhlt (andy)
  */
case class CallHistoryNormalizer() extends Normalizer[SearchHit, Map[String, Any]] {
  override def toRecord(searchHit: SearchHit, total: Long): Option[Map[String, Any]] = {
    val jsonDoc = JsonUtils.fromJson[JsonNode](searchHit.getSourceAsString)

    val callHistoryId = searchHit.getId
    val direction = jsonDoc.at("/direction").asText("")
    val source = jsonDoc.at("/source").asText("")
    val destination = jsonDoc.at("/destination").asText("")
    val time = jsonDoc.at("/time").asLong(0)
    val tta = jsonDoc.at("/tta").asInt(0)
    val duration = jsonDoc.at("/duration").asInt(0)
    val recordingFile = jsonDoc.at("/recording_file").asText("")
    val status = jsonDoc.at("/status").asText("")

    val additionalInfo = Some(jsonDoc.at("/additional_info"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .getOrElse(JsonUtils.toJsonNode("{}"))

    val group = jsonDoc.at("/group").asText("")
    val agent = Some(jsonDoc.at("/agent"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .getOrElse(JsonUtils.toJsonNode("{}"))
    val contact = Some(jsonDoc.at("/contact"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .getOrElse(JsonUtils.toJsonNode("{}"))
    val sip = Some(jsonDoc.at("/sip"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .getOrElse(JsonUtils.toJsonNode("{}"))
    val callService = jsonDoc.at("/call_service").asText("")
    val internalNumber = jsonDoc.at("/internal_number").asText("")
    val updatedTime = jsonDoc.at("/updated_time").asLong(0)
    val createdTime = jsonDoc.at("/created_time").asLong(0)

    val dataMap = Map(
      "call_history_id" -> callHistoryId,
      "direction" -> direction,
      "source" -> source,
      "destination" -> destination,
      "time" -> time,
      "tta" -> tta,
      "duration" -> duration,
      "recording_file" -> recordingFile,
      "status" -> status,
      "additional_info" -> additionalInfo,
      "group" -> group,
      "agent" -> agent,
      "contact" -> contact,
      "sip" -> sip,
      "call_service" -> callService,
      "internal_number" -> internalNumber,
      "updated_time" -> updatedTime,
      "created_time" -> createdTime
    )

    Some(dataMap)
  }
}
