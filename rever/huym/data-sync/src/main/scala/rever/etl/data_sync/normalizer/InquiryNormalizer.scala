package rever.etl.data_sync.normalizer

import com.fasterxml.jackson.databind.JsonNode
import org.elasticsearch.search.SearchHit
import rever.etl.data_sync.core.Normalizer
import rever.etl.data_sync.domain.rever_search.InquiryDM
import rever.etl.rsparkflow.utils.JsonUtils

import scala.jdk.CollectionConverters.asScalaIteratorConverter

/** @author anhlt (andy)
  */
case class InquiryNormalizer() extends Normalizer[SearchHit, Map[String, Any]] {

  override def toRecord(searchHit: SearchHit, total: Long): Option[Map[String, Any]] = {
    val inquiryNode = JsonUtils.fromJson[JsonNode](searchHit.getSourceAsString)

    val inquiryId = searchHit.getId
    val inquiryCode = inquiryNode.at("/inquiry_code").asText("")
    val name = inquiryNode.at("/name").asText("")
    val cid = inquiryNode.at("/cid").asText("")
    val pCid = inquiryNode.at("/p_cid").asText("")
    val description = inquiryNode.at("/description").asText("")

    val serviceType = inquiryNode.at("/service_type").asInt(0)
    val address = inquiryNode.at("/address") match {
      case node if node.isArray =>
        node
          .elements()
          .asScala
          .toSeq
          .filterNot(_.isNull)
          .filterNot(_.isMissingNode)
          .map(_.toString)
          .filterNot(_ == null)
          .filterNot(_.isEmpty)
          .toArray
      case node if !node.isMissingNode && !node.isNull =>
        Array(node.toString)
      case _ => Array.empty[String]
    }

    val minPrice = inquiryNode.at("/min_price").asDouble(0.0)
    val maxPrice = inquiryNode.at("/max_price").asDouble(0.0)
    val minArea = inquiryNode.at("/min_area").asDouble(0.0)
    val maxArea = inquiryNode.at("/max_area").asDouble(0.0)
    val minBed = inquiryNode.at("/min_bed").asInt(0)
    val maxBed = inquiryNode.at("/max_bed").asInt(0)
    val minToilet = inquiryNode.at("/min_toilet").asInt(0)
    val maxToilet = inquiryNode.at("/max_toilet").asInt(0)

    val propertyTypes = inquiryNode
      .at("/property_types")
      .elements()
      .asScala
      .map(_.asInt())
      .toArray

    val directions = inquiryNode
      .at("/directions")
      .elements()
      .asScala
      .map(_.asInt())
      .toArray
    val balconyDirections = inquiryNode
      .at("/balcony_directions")
      .elements()
      .asScala
      .map(_.asInt())
      .toArray

    val ownerId = inquiryNode.at("/owner").asText("")
    val teamId = inquiryNode.at("/owner_team").asText("")
    val marketCenterId = inquiryNode.at("/owner_mc").asText("")
    val createdBy = inquiryNode.at("/created_by").asText("")
    val updatedBy = inquiryNode.at("/updated_by").asText("")
    val createdTime = inquiryNode.at("/created_time").asLong(0L)
    val updatedTime = inquiryNode.at("/updated_time").asLong(0L)

    val status = inquiryNode.at("/status").asInt(0)
    val verified = inquiryNode.at("/verified").asInt(0)

    val dataMap = Map(
      InquiryDM.ID -> inquiryId,
      InquiryDM.INQUIRY_CODE -> inquiryCode,
      InquiryDM.NAME -> name,
      InquiryDM.DESCRIPTION -> description,
      InquiryDM.CID -> cid,
      InquiryDM.P_CID -> pCid,
      InquiryDM.SERVICE_TYPE -> serviceType,
      InquiryDM.PROPERTY_TYPES -> propertyTypes,
      InquiryDM.MIN_PRICE -> minPrice,
      InquiryDM.MAX_PRICE -> maxPrice,
      InquiryDM.MIN_AREA -> minArea,
      InquiryDM.MAX_AREA -> maxArea,
      InquiryDM.MIN_BED -> minBed,
      InquiryDM.MAX_BED -> maxBed,
      InquiryDM.MIN_TOILET -> minToilet,
      InquiryDM.MAX_TOILET -> maxToilet,
      InquiryDM.DIRECTIONS -> directions,
      InquiryDM.BALCONY_DIRECTIONS -> balconyDirections,
      InquiryDM.ADDRESS -> address,
      InquiryDM.OWNER_ID -> ownerId,
      InquiryDM.TEAM_ID -> teamId,
      InquiryDM.MARKET_CENTER_ID -> marketCenterId,
      InquiryDM.CREATED_BY -> createdBy,
      InquiryDM.UPDATED_BY -> updatedBy,
      InquiryDM.CREATED_TIME -> createdTime,
      InquiryDM.UPDATED_TIME -> updatedTime,
      InquiryDM.STATUS -> status,
      InquiryDM.VERIFIED -> verified,
      InquiryDM.LOG_TIME -> System.currentTimeMillis()
    )

    Some(dataMap)
  }
}
