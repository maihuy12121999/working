package rever.etl.data_sync.normalizer

import com.fasterxml.jackson.databind.JsonNode
import org.elasticsearch.search.SearchHit
import rever.etl.data_sync.core.Normalizer
import rever.etl.rsparkflow.utils.JsonUtils

/** @author anhlt (andy)
  */
case class PropertyNormalizer() extends Normalizer[SearchHit, Map[String, Any]] {

  override def toRecord(searchHit: SearchHit, total: Long): Option[Map[String, Any]] = {
    val jsonDoc = JsonUtils.fromJson[JsonNode](searchHit.getSourceAsString)

    val propertyId = searchHit.getId
    val alias = jsonDoc.at("/alias").asText("")
    val reverId = jsonDoc.at("/rever_id").asText("")
    val name = jsonDoc.at("/name").asText("")
    val fullName = jsonDoc.at("/full_name").asText("")
    val transaction = Some(jsonDoc.at("/transaction"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .getOrElse(JsonUtils.toJsonNode("{}"))
    val address = Some(jsonDoc.at("/address"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .getOrElse(JsonUtils.toJsonNode("{}"))
    val numBedRoom = jsonDoc.at("/num_bed_room").asInt(0)
    val numBathRoom = jsonDoc.at("/num_bath_room").asInt(0)
    val area = jsonDoc.at("/area").asDouble(0.0)
    val serviceType = jsonDoc.at("/service_type").asInt(0)
    val propertyType = jsonDoc.at("/property_type").asInt(0)
    val creator = jsonDoc.at("/creator").asText("")
    val ownerId = jsonDoc.at("/owner_id").asText("")
    val createdTime = jsonDoc.at("/created_time").asLong(0L)
    val updatedTime = jsonDoc.at("/updated_time").asLong(0L)
    val publishedTime = jsonDoc.at("/published_time").asLong(0L)
    val signedTime = jsonDoc.at("/signed_time").asLong(0L)
    val propertyNotes = jsonDoc.at("/property_notes").asText("")
    val contact = Some(jsonDoc.at("/contact"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .getOrElse(JsonUtils.toJsonNode("{}"))
    val exclusive = jsonDoc.at("/exclusive").asBoolean(false) match {
      case true => 1
      case _    => 0
    }
    val isHot = jsonDoc.at("/is_hot").asBoolean(false) match {
      case true => 1
      case _    => 0
    }
    val projectId = jsonDoc.at("/project_id").asText("")
    val areaUsing = jsonDoc.at("/area_using").asDouble(0.0)
    val areaBuilding = jsonDoc.at("/area_building").asDouble(0.0)
    val width = jsonDoc.at("/width").asDouble(0.0)
    val length = jsonDoc.at("/length").asDouble(0.0)
    val floors = jsonDoc.at("/floors").asInt(0)
    val ownership = jsonDoc.at("/ownership").asText("")
    val buildingTime = jsonDoc.at("/building_time").asLong(0L)
    val architecturalStyle = jsonDoc.at("/architectural_style").asInt(0)
    val direction = jsonDoc.at("/direction").asInt(0)
    val balconyDirection = jsonDoc.at("/balcony_direction").asInt(0)

    val additionalInfo = Some(jsonDoc.at("/additional_info"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .getOrElse(JsonUtils.toJsonNode("{}"))

    val amenities = Some(jsonDoc.at("/amenities"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .getOrElse(JsonUtils.toJsonNode("{}"))

    val furniture = Some(jsonDoc.at("/furniture"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .getOrElse(JsonUtils.toJsonNode("{}"))

    val juridicalStatus = jsonDoc.at("/juridical_status").asInt(0)
    val houseStatus = jsonDoc.at("/house_status").asInt(0)
    val furnitureStatus = jsonDoc.at("/furniture_status").asText("")

    val teamId = jsonDoc.at("/team_id").asText("")
    val marketCenterId = jsonDoc.at("/market_center").asText("")
    val keyLockNumber = jsonDoc.at("/key_lock_number").asText("")

    val alleywayWidth = jsonDoc.at("/alleyway_width").asDouble(0.0)
    val listingOpportunityId = jsonDoc.at("/listing_opportunity_id").asLong(0L)
    val propertyStatus = jsonDoc.at("/property_status").asLong(0L)
    val saleDiscountVnd = jsonDoc.at("/sale_discount_vnd").asDouble(0.0)
    val rentDiscountVnd = jsonDoc.at("/rent_discount_vnd").asDouble(0.0)

    val dataMap = Map(
      "property_id" -> propertyId,
      "alias" -> alias,
      "rever_id" -> reverId,
      "name" -> name,
      "full_name" -> fullName,
      "transaction" -> transaction,
      "address" -> address,
      "num_bed_room" -> numBedRoom,
      "num_bath_room" -> numBathRoom,
      "area" -> area,
      "service_type" -> serviceType,
      "property_type" -> propertyType,
      "creator" -> creator,
      "owner_id" -> ownerId,
      "created_time" -> createdTime,
      "updated_time" -> updatedTime,
      "published_time" -> publishedTime,
      "signed_time" -> signedTime,
      "property_notes" -> propertyNotes,
      "contact" -> contact,
      "exclusive" -> exclusive,
      "is_hot" -> isHot,
      "project_id" -> projectId,
      "area_using" -> areaUsing,
      "area_building" -> areaBuilding,
      "width" -> width,
      "length" -> length,
      "floors" -> floors,
      "ownership" -> ownership,
      "building_time" -> buildingTime,
      "architectural_style" -> architecturalStyle,
      "direction" -> direction,
      "balcony_direction" -> balconyDirection,
      "additional_info" -> additionalInfo,
      "amenities" -> amenities,
      "furniture" -> furniture,
      "juridical_status" -> juridicalStatus,
      "house_status" -> houseStatus,
      "furniture_status" -> furnitureStatus,
      "team_id" -> teamId,
      "market_center_id" -> marketCenterId,
      "key_lock_number" -> keyLockNumber,
      "alleyway_width" -> alleywayWidth,
      "listing_opportunity_id" -> listingOpportunityId,
      "sale_discount_vnd" -> saleDiscountVnd,
      "rent_discount_vnd" -> rentDiscountVnd,
      "property_status" -> propertyStatus
    )

    Some(dataMap)
  }
}
