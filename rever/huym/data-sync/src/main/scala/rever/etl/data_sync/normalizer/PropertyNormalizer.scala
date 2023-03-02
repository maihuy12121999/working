package rever.etl.data_sync.normalizer

import com.fasterxml.jackson.databind.JsonNode
import org.elasticsearch.search.SearchHit
import rever.data_health.domain.ListingHealthEvaluator
import rever.data_health.domain.listing.ListingHealthItemBuilder
import rever.data_health.utils.ListingDataHealthUtils
import rever.etl.data_sync.core.Normalizer
import rever.etl.data_sync.domain.rever_search.PropertyDM
import rever.etl.rsparkflow.utils.JsonUtils
import vn.rever.common.util.JsonHelper

/** @author anhlt (andy)
  */
case class PropertyNormalizer(
    listingHealthEvaluator: ListingHealthEvaluator
) extends Normalizer[SearchHit, Map[String, Any]] {

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
      .map(_.toString)
      .getOrElse("{}")

    val address = Some(jsonDoc.at("/address"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .map(_.toString)
      .getOrElse("{}")

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
    val propertyNotes = jsonDoc.at("/contact/contacted_notes").asText("").trim
    val contact = Some(jsonDoc.at("/contact"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .map(_.toString)
      .getOrElse("{}")

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
      .map(_.toString)
      .getOrElse("{}")

    val amenities = Some(jsonDoc.at("/amenities"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .map(_.toString)
      .getOrElse("{}")

    val furniture = Some(jsonDoc.at("/furniture"))
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .map(_.toString)
      .getOrElse("{}")

    val juridicalStatus = jsonDoc.at("/juridical_status").asInt(0)
    val houseStatus = jsonDoc.at("/house_status").asInt(0)
    val furnitureStatus = jsonDoc.at("/furniture_status").asText("").trim
    val saleStatus = jsonDoc.at("/sale_status").asInt(0)

    val teamId = jsonDoc.at("/team_id").asText("")
    val marketCenterId = jsonDoc.at("/market_center").asText("")
    val keyLockNumber = jsonDoc.at("/key_lock_number").asText("").trim

    val alleywayWidth = jsonDoc.at("/alleyway_width").asDouble(0.0)
    val listingOpportunityId = jsonDoc.at("/listing_opportunity_id").asLong(0L)
    val propertyStatus = jsonDoc.at("/property_status").asLong(0L)
    val publishedStatus = jsonDoc.at("/transaction/stage").asLong(0L)
    val saleDiscountVnd = jsonDoc.at("/sale_discount_vnd").asDouble(0.0)
    val rentDiscountVnd = jsonDoc.at("/rent_discount_vnd").asDouble(0.0)
    val salePriceVnd = jsonDoc.at("/sell_price_vnd").asDouble(0.0)
    val rentPriceVnd = jsonDoc.at("/rental_price_vnd").asDouble(0.0)

    val contentAssignee = jsonDoc.at("/content_assignee").asText("")
    val contentAssigner = jsonDoc.at("/content_assigner").asText("")
    val contentStatus = jsonDoc.at("/content_status").asInt(0)
    val requestContentTime = jsonDoc.at("/request_content_time").asLong(0L)

    val mediaAssignee = jsonDoc.at("/media_assignee").asText("")
    val mediaAssigner = jsonDoc.at("/media_assigner").asText("")
    val mediaStatus = jsonDoc.at("/media_status").asInt(0)
    val virtualTourUrl = Option(jsonDoc.at("/additional_info/media/virtual_tour/url").asText())
    // Listing Score
    val transactionStage = JsonUtils.fromJson[JsonNode](transaction).at("/stage").asInt(-1)
    val (amenitiesMap, furnitureMap) = (
      ListingDataHealthUtils.getAmenitiesOrFurnitureMap(Some(JsonUtils.fromJson[JsonNode](amenities))),
      ListingDataHealthUtils.getAmenitiesOrFurnitureMap(Some(JsonUtils.fromJson[JsonNode](furniture)))
    )

    val listingHealthItem = ListingHealthItemBuilder()
      .setPropertyType(propertyType)
      .setFurnitureStatus(Option(furnitureStatus))
      .withIsPublished(Some(propertyStatus == 700 && transactionStage == 10))
      .withDirection(Some(direction))
      .withBalconyDirection(Some(balconyDirection))
      .withAlleyWidth(Some(alleywayWidth))
      .withDescription(Option(jsonDoc.at("/additional_info/desc/overview").asText()))
      .withAmenities(ListingDataHealthUtils.getActualAmenities(amenitiesMap, furnitureMap))
      .withFurniture(ListingDataHealthUtils.getActualFurniture(amenitiesMap, furnitureMap))
      .withDevices(ListingDataHealthUtils.getActualDevices(amenitiesMap, furnitureMap))
      .withNotes(Option(propertyNotes))
      .withPhotos(ListingDataHealthUtils.getActualPhotos(jsonDoc.at("/additional_info/photos")))
      .withVirtualTourUrl(virtualTourUrl)
      .withKeyLockNumber(Option(keyLockNumber))
      .build()
    val isVerified = isListingVerified(virtualTourUrl)

    val listingScoringLevel = listingHealthItem.listingScoringLevel.map(_.id).getOrElse(0)

    val dataHealthScore = listingHealthEvaluator.evaluate(listingHealthItem)

    val dataMap = Map(
      PropertyDM.PROPERTY_ID -> propertyId,
      PropertyDM.ALIAS -> alias,
      PropertyDM.REVER_ID -> reverId,
      PropertyDM.NAME -> name,
      PropertyDM.FULL_NAME -> fullName,
      PropertyDM.SERVICE_TYPE -> serviceType,
      PropertyDM.PROPERTY_TYPE -> propertyType,
      PropertyDM.TRANSACTION -> transaction,
      PropertyDM.SALE_DISCOUNT_VND -> saleDiscountVnd,
      PropertyDM.RENT_DISCOUNT_VND -> rentDiscountVnd,
      PropertyDM.SALE_PRICE_VND -> salePriceVnd,
      PropertyDM.RENT_PRICE_VND -> rentPriceVnd,
      PropertyDM.NUM_BED_ROOM -> numBedRoom,
      PropertyDM.NUM_BATH_ROOM -> numBathRoom,
      PropertyDM.AREA -> area,
      PropertyDM.AREA_USING -> areaUsing,
      PropertyDM.AREA_BUILDING -> areaBuilding,
      PropertyDM.ALLEY_WAY_WIDTH -> alleywayWidth,
      PropertyDM.WIDTH -> width,
      PropertyDM.LENGTH -> length,
      PropertyDM.FLOORS -> floors,
      PropertyDM.EXCLUSIVE -> exclusive,
      PropertyDM.IS_HOT -> isHot,
      PropertyDM.PROJECT_ID -> projectId,
      PropertyDM.ARCHITECTURAL_STYLE -> architecturalStyle,
      PropertyDM.DIRECTION -> direction,
      PropertyDM.BALCONY_DIRECTION -> balconyDirection,
      PropertyDM.AMENITIES -> amenities,
      PropertyDM.FURNITURE -> furniture,
      PropertyDM.OWNERSHIP -> ownership,
      PropertyDM.CONTACT -> contact,
      PropertyDM.ADDRESS -> address,
      PropertyDM.ADDITIONAL_INFO -> additionalInfo,
      PropertyDM.OWNER_ID -> ownerId,
      PropertyDM.TEAM_ID -> teamId,
      PropertyDM.MARKET_CENTER_ID -> marketCenterId,
      PropertyDM.PROPERTY_NOTES -> propertyNotes,
      PropertyDM.KEY_LOCK_NUMBER -> keyLockNumber,
      PropertyDM.LISTING_OPPO_ID -> listingOpportunityId,
      PropertyDM.FURNITURE_STATUS -> furnitureStatus,
      PropertyDM.JURIDICAL_STATUS -> juridicalStatus,
      PropertyDM.HOUSE_STATUS -> houseStatus,
      PropertyDM.PROPERTY_STATUS -> propertyStatus,
      PropertyDM.PUBLISHED_STATUS -> publishedStatus,
      PropertyDM.SALE_STATUS -> saleStatus,
      PropertyDM.IS_VERIFIED -> isVerified,
      PropertyDM.CONTENT_ASSIGNEE -> contentAssignee,
      PropertyDM.CONTENT_ASSIGNER -> contentAssigner,
      PropertyDM.CONTENT_STATUS -> contentStatus,
      PropertyDM.REQUEST_CONTENT_TIME -> requestContentTime,
      PropertyDM.MEDIA_ASSIGNEE -> mediaAssignee,
      PropertyDM.MEDIA_ASSIGNER -> mediaAssigner,
      PropertyDM.MEDIA_STATUS -> mediaStatus,
      PropertyDM.CREATOR -> creator,
      PropertyDM.CREATED_TIME -> createdTime,
      PropertyDM.UPDATED_TIME -> updatedTime,
      PropertyDM.PUBLISHED_TIME -> publishedTime,
      PropertyDM.SIGNED_TIME -> signedTime,
      PropertyDM.BUILDING_TIME -> buildingTime,
      PropertyDM.LOG_TIME -> System.currentTimeMillis()
    )

    Some(
      dataMap ++ Map(
        PropertyDM.DATA_HEALTH_SCORING_LEVEL -> listingScoringLevel,
        PropertyDM.DATA_HEALTH_SCORE -> dataHealthScore.score,
        PropertyDM.DATA_HEALTH_INFO -> JsonHelper.toJson(dataHealthScore.fields, pretty = false)
      )
    )
  }

  private def isListingVerified(virtualTourUrl: Option[String]): Boolean = {
    !virtualTourUrl.filterNot(_ == null).forall(_.isEmpty)
  }

}



