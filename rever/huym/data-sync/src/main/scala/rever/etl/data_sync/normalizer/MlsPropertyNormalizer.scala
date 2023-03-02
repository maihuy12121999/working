package rever.etl.data_sync.normalizer

import com.fasterxml.jackson.databind.JsonNode
import org.elasticsearch.search.SearchHit
import rever.etl.data_sync.core.Normalizer
import rever.etl.rsparkflow.utils.JsonUtils

/** @author anhlt (andy)
  */
case class MlsPropertyNormalizer() extends Normalizer[SearchHit, Map[String, Any]] {

  override def toRecord(searchHit: SearchHit, total: Long): Option[Map[String, Any]] = {
    val jsonDoc = JsonUtils.fromJson[JsonNode](searchHit.getSourceAsString)

    val id = searchHit.getId
    val mlsId = jsonDoc.at("/mls_id").asText("")
    val name = jsonDoc.at("/name").asText("")
    val serviceType = jsonDoc.at("/service_type").asInt(0)
    val propertyType = jsonDoc.at("/property_type").asInt(0)
    val numBedRoom = jsonDoc.at("/num_bed_room").asInt(0)
    val numBathRoom = jsonDoc.at("/num_bath_room").asInt(0)
    val area = jsonDoc.at("/area").asDouble(0.0)
    val areaUsing = jsonDoc.at("/area_using").asDouble(0.0)
    val width = jsonDoc.at("/width").asDouble(0.0)
    val length = jsonDoc.at("/length").asDouble(0.0)
    val floors = jsonDoc.at("/total_floors").asInt(0)
    val direction = jsonDoc.at("/direction").asInt(0)
    val sellPriceVnd = jsonDoc.at("/sell_price_vnd").asDouble(0.0)
    val projectName = jsonDoc.at("/project_name").asText("")
    val address = Some(jsonDoc.at("/address"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .map(_.toString)
      .getOrElse("{}")
    val contact = Some(jsonDoc.at("/contact"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .map(_.toString)
      .map(x => secureContactInfo(JsonUtils.fromJson[JsonNode](x)))
      .getOrElse("{}")
    val media = Some(jsonDoc.at("/media"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .map(_.toString)
      .getOrElse("{}")
    val review = Some(jsonDoc.at("/review"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .map(_.toString)
      .getOrElse("{}")
    val marketing = Some(jsonDoc.at("/marketing"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .map(_.toString)
      .getOrElse("{}")
    val originalInfo = Some(jsonDoc.at("/original_info"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .map(_.toString)
      .getOrElse("{}")

    val crawledTime = jsonDoc.at("/crawled_time").asLong(0L)
    val createdTime = jsonDoc.at("/created_time").asLong(0L)
    val updatedTime = jsonDoc.at("/updated_time").asLong(0L)
    val updatedBy = jsonDoc.at("/updated_by").asText("")
    val status = jsonDoc.at("/status").asInt(0)
    val assignee = jsonDoc.at("/assignee").asText("")

    val scoreInfo = Some(jsonDoc.at("/score_info"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .map(_.toString)
      .getOrElse("{}")

    val dataMap = Map(
      "id" -> id,
      "mls_id" -> mlsId,
      "name" -> name,
      "service_type" -> serviceType,
      "property_type" -> propertyType,
      "num_bed_room" -> numBedRoom,
      "num_bath_room" -> numBathRoom,
      "area" -> area,
      "area_using" -> areaUsing,
      "width" -> width,
      "length" -> length,
      "total_floors" -> floors,
      "direction" -> direction,
      "sell_price_vnd" -> sellPriceVnd,
      "project_name" -> projectName,
      "address" -> address,
      "contact" -> contact,
      "media" -> media,
      "review" -> review,
      "marketing" -> marketing,
      "original_info" -> originalInfo,
      "crawled_time" -> crawledTime,
      "created_time" -> createdTime,
      "updated_time" -> updatedTime,
      "updated_by" -> updatedBy,
      "status" -> status,
      "assignee" -> assignee,
      "score_info" -> scoreInfo,
      "log_time" -> System.currentTimeMillis()
    )

    Some(dataMap)
  }

  private def secureContactInfo(contactNode: JsonNode): String = {
    val map = JsonUtils.fromJson[Map[String, Any]](contactNode.toPrettyString)

    val secureMap = map
      .filterNot(_._1.equals("phone"))
      .filterNot(_._1.equals("email"))

    JsonUtils.toJson(secureMap, pretty = false)

  }
}
