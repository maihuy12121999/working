package rever.etl.data_sync.normalizer

import com.fasterxml.jackson.databind.JsonNode
import org.elasticsearch.search.SearchHit
import rever.etl.data_sync.core.Normalizer
import rever.etl.rsparkflow.utils.JsonUtils

/** @author anhlt (andy)
  */
case class ProjectTowerNormalizer() extends Normalizer[SearchHit, Map[String, Any]] {

  override def toRecord(searchHit: SearchHit, total: Long): Option[Map[String, Any]] = {
    val jsonDoc = JsonUtils.fromJson[JsonNode](searchHit.getSourceAsString)

    val towerId = searchHit.getId
    val projectId = jsonDoc.at("/project_id").asText("")
    val towerCode = jsonDoc.at("/tower_code").asText("")
    val name = jsonDoc.at("/name").asText("")
    val towerStatus = jsonDoc.at("/tower_status").asInt(0)
    val displayStatus = jsonDoc.at("/display_status").asInt(0)
    val numFloor = jsonDoc.at("/num_floor").asInt(0)
    val numUnit = jsonDoc.at("/num_unit").asInt(0)
    val numElevator = jsonDoc.at("/num_elevator").asInt(0)
    val unitType = Some(jsonDoc.at("/unit_type"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .map(_.toString)
      .getOrElse("[]")

    val geoLocation = Some(jsonDoc.at("/geolocation"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .getOrElse(JsonUtils.toJsonNode("{}"))
    val additionalInfo = Some(jsonDoc.at("/additional_info"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .getOrElse(JsonUtils.toJsonNode("{}"))

    val finishedTime = jsonDoc.at("/finished_time").asLong(0L)
    val createdTime = jsonDoc.at("/created_time").asLong(0L)
    val updatedTime = jsonDoc.at("/updated_time").asLong(0L)
    val creator = jsonDoc.at("/creator").asText("")
    val updatedBy = jsonDoc.at("/updated_by").asText("")

    val dataMap = Map(
      "tower_id" -> towerId,
      "tower_code" -> Seq(towerCode, name).filterNot(_ == null).filterNot(_.isEmpty).headOption.getOrElse(""),
      "project_id" -> projectId,
      "name" -> name,
      "unit_type" -> unitType,
      "num_floor" -> numFloor,
      "num_unit" -> numUnit,
      "num_elevator" -> numElevator,
      "geolocation" -> geoLocation,
      "additional_info" -> additionalInfo,
      "creator" -> creator,
      "updated_by" -> updatedBy,
      "created_time" -> createdTime,
      "updated_time" -> updatedTime,
      "finished_time" -> finishedTime,
      "tower_status" -> towerStatus,
      "display_status" -> displayStatus,
      "log_time" -> System.currentTimeMillis()
    )

    Some(dataMap)
  }
}
