package rever.etl.data_sync.domain.contact

import com.fasterxml.jackson.databind.JsonNode
import rever.etl.data_sync.domain.GenericRecord
import rever.etl.data_sync.util.Utils.ImplicitAny
import rever.etl.rsparkflow.utils.JsonUtils
import vn.rever.jdbc.{JdbcRecord, SqlFieldMissing}

import scala.jdk.CollectionConverters.asScalaIteratorConverter


object SysContact {
  final val TBL_NAME = "contact"

  final val ID = "id"
  final val CID = "cid"
  final val FIRST_NAME = "first_name"
  final val LAST_NAME = "last_name"
  final val FULL_NAME = "full_name"
  final val PHONE = "phone"
  final val EMAIL = "email"
  final val AVATAR = "avatar"
  final val CONTACT_SOURCE = "contact_source"
  final val C_CONTACT_SOURCE = "c_contact_source"
  final val HUBSPOT_IDS = "hubspot_ids"
  final val PROPERTIES = "properties"
  final val CREATED_BY = "created_by"
  final val UPDATED_BY = "updated_by"
  final val CREATED_TIME = "created_time"
  final val UPDATED_TIME = "updated_time"
  final val STATUS = "status"

  final val PRIMARY_IDS = Seq(ID)

  val FIELDS = Seq(
    ID,
    CID,
    FIRST_NAME,
    LAST_NAME,
    FULL_NAME,
    PHONE,
    EMAIL,
    AVATAR,
    CONTACT_SOURCE,
    C_CONTACT_SOURCE,
    HUBSPOT_IDS,
    PROPERTIES,
    CREATED_BY,
    UPDATED_BY,
    UPDATED_TIME,
    CREATED_TIME,
    STATUS
  )

  final def fromGenericRecord(record: GenericRecord): SysContact = {
    val contact = SysContact()

    record
      .getFields()
      .map(field => field -> record.getValue(field))
      .filter(_._2.nonEmpty)
      .foreach { case (field, valueOpt) =>
        contact.setValues(field, valueOpt)
      }
    contact
  }

}

case class SysContact(
    var id: Option[Long] = None,
    var cid: Option[String] = None,
    var firstName: Option[String] = None,
    var lastName: Option[String] = None,
    var fullName: Option[String] = None,
    var phone: Option[String] = None,
    var email: Option[Seq[String]] = None,
    var avatar: Option[String] = None,
    var contactSource: Option[String] = None,
    var cContactSource: Option[String] = None,
    var hubspotIds: Option[Seq[String]] = None,
    var properties: Option[JsonNode] = None,
    var createdBy: Option[String] = None,
    var updatedBy: Option[String] = None,
    var createdTime: Option[Long] = None,
    var updatedTime: Option[Long] = None,
    var status: Option[Int] = None
) extends JdbcRecord {

  override def getPrimaryKeys(): Seq[String] = SysContact.PRIMARY_IDS
  override def getFields(): Seq[String] = SysContact.FIELDS

  override def setValues(field: String, value: Any): Unit = field match {
    case SysContact.ID               => id = value.asOpt
    case SysContact.CID              => cid = value.asOpt
    case SysContact.FIRST_NAME       => firstName = value.asOpt
    case SysContact.LAST_NAME        => lastName = value.asOpt
    case SysContact.FULL_NAME        => fullName = value.asOpt
    case SysContact.PHONE            => phone = value.asOpt
    case SysContact.EMAIL            => email = value.asOptString.map(JsonUtils.fromJson[Seq[String]])
    case SysContact.AVATAR           => avatar = value.asOpt
    case SysContact.CONTACT_SOURCE   => contactSource = value.asOpt
    case SysContact.C_CONTACT_SOURCE => cContactSource = value.asOpt
    case SysContact.HUBSPOT_IDS      => hubspotIds = value.asOptString.map(JsonUtils.fromJson[Seq[String]])
    case SysContact.PROPERTIES =>
      properties = value.asOptString
        .map(JsonUtils.fromJson[JsonNode])
        .filterNot(_.isEmpty)
        .filterNot(_.isNull)
        .filterNot(_.isMissingNode)
    case SysContact.CREATED_BY   => createdBy = value.asOpt
    case SysContact.UPDATED_BY   => updatedBy = value.asOpt
    case SysContact.CREATED_TIME => createdTime = value.asOpt
    case SysContact.UPDATED_TIME => updatedTime = value.asOpt
    case SysContact.STATUS       => status = value.asOpt
    case _                       =>
  }

  override def getValue(field: String): Option[Any] = field match {
    case SysContact.ID               => id
    case SysContact.CID              => cid
    case SysContact.FIRST_NAME       => firstName
    case SysContact.LAST_NAME        => lastName
    case SysContact.FULL_NAME        => fullName
    case SysContact.PHONE            => phone
    case SysContact.EMAIL            => email.map(JsonUtils.toJson(_, pretty = false))
    case SysContact.AVATAR           => avatar
    case SysContact.CONTACT_SOURCE   => contactSource
    case SysContact.C_CONTACT_SOURCE => cContactSource
    case SysContact.HUBSPOT_IDS      => hubspotIds.map(JsonUtils.toJson(_, pretty = false))
    case SysContact.PROPERTIES       => properties.map(_.toString)
    case SysContact.CREATED_BY       => createdBy
    case SysContact.UPDATED_BY       => updatedBy
    case SysContact.CREATED_TIME     => createdTime
    case SysContact.UPDATED_TIME     => updatedTime
    case SysContact.STATUS           => status
    case _                           => throw SqlFieldMissing(field)
  }

  def toMap: Map[String, Any] = {
    SysContact.FIELDS
      .map(field => field -> getValue(field).get)
      .toMap
  }

  def cSegmentTags: Seq[String] = {
    properties
      .map(_.at("/c_segment_tags"))
      .filterNot(_.isNull)
      .filterNot(_.isEmpty)
      .filterNot(_.isMissingNode)
      .filter(_.isArray)
      .map(_.elements().asScala.toSeq)
      .getOrElse(Seq.empty)
      .map(_.asText())

  }

  def personalTitle: Option[String] = {
    properties
      .map(_.at("/danh_xung"))
      .filterNot(_.isNull)
      .map(_.asText(""))
      .filterNot(_.isEmpty)

  }

  def gender: Option[String] = {
    properties
      .map(_.at("/gender"))
      .filterNot(_.isNull)
      .map(_.asText(""))
      .filterNot(_.isEmpty)

  }

}
