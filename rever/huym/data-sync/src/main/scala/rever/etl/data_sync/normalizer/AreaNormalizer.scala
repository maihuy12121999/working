package rever.etl.data_sync.normalizer

import com.fasterxml.jackson.databind.JsonNode
import org.elasticsearch.search.SearchHit
import rever.etl.data_sync.core.Normalizer
import rever.etl.data_sync.domain.rever_search.Point
import rever.etl.data_sync.util.CentralPointUtils
import rever.etl.rsparkflow.utils.JsonUtils

/** @author anhlt (andy)
  */
private case class AdditionalInfo(boundaries: List[Point])
case class AreaNormalizer() extends Normalizer[SearchHit, Map[String, Any]] {

  override def toRecord(searchHit: SearchHit, total: Long): Option[Map[String, Any]] = {
    val jsonDoc = JsonUtils.fromJson[JsonNode](searchHit.getSourceAsString)

    val areaId = searchHit.getId

    val areaType = jsonDoc.at("/area_type").asInt(0)
    val areaLevel = jsonDoc.at("/area_level").asInt(0)
    val alias = jsonDoc.at("/alias").asText("")
    val name = jsonDoc.at("/name").asText("")
    val fullName = jsonDoc.at("/full_name").asText("")
    val keyword = jsonDoc.at("/keyword").asText("")

    val address = Some(jsonDoc.at("/address"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .map(_.toString)
      .getOrElse("{}")

    val additionalInfo = Some(jsonDoc.at("/additional_info"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .map(_.toString)
      .getOrElse("{}")

    val points = Some(JsonUtils.fromJson[AdditionalInfo](additionalInfo).boundaries)
      .filterNot(_ == null)
      .getOrElse(List.empty[Point])

    val centralPoint = if (points.isEmpty) "{}" else JsonUtils.toJson(CentralPointUtils.getCenterPoint(points), false)
    val creator = jsonDoc.at("/creator").asText("")
    val updatedBy = jsonDoc.at("/updated_by").asText("")
    val createdTime = jsonDoc.at("/created_time").asLong(0L)
    val updatedTime = jsonDoc.at("/updated_time").asLong(0L)

    val status = jsonDoc.at("/status").asInt(0)

    val dataMap = Map(
      "area_id" -> areaId,
      "area_type" -> areaType,
      "area_level" -> areaLevel,
      "alias" -> alias,
      "name" -> name,
      "full_name" -> fullName,
      "keyword" -> keyword,
      "address" -> address,
      "additional_info" -> additionalInfo,
      "central_point" -> centralPoint,
      "creator" -> creator,
      "updated_by" -> updatedBy,
      "status" -> status,
      "created_time" -> createdTime,
      "updated_time" -> updatedTime,
      "log_time" -> System.currentTimeMillis()
    )

    Some(dataMap)
  }
}
