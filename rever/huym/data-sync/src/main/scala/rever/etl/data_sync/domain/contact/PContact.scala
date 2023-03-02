package rever.etl.data_sync.domain.contact

import com.fasterxml.jackson.databind.JsonNode
import rever.etl.data_sync.domain.GenericRecord
import rever.etl.data_sync.util.Utils.ImplicitAny
import rever.etl.rsparkflow.utils.JsonUtils
import vn.rever.jdbc.{JdbcRecord, SqlFieldMissing}

object PContact {
  final val TBL_NAME = "personal_contact"

  final val ID = "id"
  final val P_CID = "p_cid"
  final val CID = "cid"
  final val NICKNAME = "nickname"
  final val FIRST_NAME = "first_name"
  final val LAST_NAME = "last_name"
  final val FULL_NAME = "full_name"
  final val PHONE = "phone"
  final val EMAIL = "email"
  final val ADDITIONAL_EMAILS = "additional_emails"
  final val AVATAR = "avatar"
  final val OWNER = "owner"
  final val OWNER_TEAM = "owner_team"
  final val OWNER_MC = "owner_market_center"
  final val ADMIN_P_CID = "admin_p_cid"
  final val CONTACT_SOURCE = "contact_source"
  final val CONTACT_STATUS = "contact_status"
  final val NOTES = "notes"
  final val PROPERTIES = "properties"
  final val CREATED_TIME = "created_time"
  final val UPDATED_TIME = "updated_time"
  final val STATUS = "status"

  final val PRIMARY_IDS = Seq(ID)

  val FIELDS = Seq(
    ID,
    P_CID,
    CID,
    NICKNAME,
    FIRST_NAME,
    LAST_NAME,
    FULL_NAME,
    PHONE,
    EMAIL,
    ADDITIONAL_EMAILS,
    AVATAR,
    OWNER,
    OWNER_TEAM,
    OWNER_MC,
    ADMIN_P_CID,
    CONTACT_SOURCE,
    CONTACT_STATUS,
    NOTES,
    PROPERTIES,
    UPDATED_TIME,
    CREATED_TIME,
    STATUS
  )

  final def fromGenericRecord(record: GenericRecord): PContact = {
    val pContact = PContact()

    record
      .getFields()
      .map(field => field -> record.getValue(field))
      .filter(_._2.nonEmpty)
      .foreach { case (field, valueOpt) =>
        pContact.setValues(field, valueOpt)
      }
    pContact
  }

}

case class PContact(
    var id: Option[Long] = None,
    var pCid: Option[String] = None,
    var cid: Option[String] = None,
    var nickname: Option[String] = None,
    var firstName: Option[String] = None,
    var lastName: Option[String] = None,
    var fullName: Option[String] = None,
    var phone: Option[String] = None,
    var email: Option[String] = None,
    var additionalEmails: Option[Seq[String]] = None,
    var avatar: Option[String] = None,
    var owner: Option[String] = None,
    var ownerTeam: Option[String] = None,
    var ownerMarketCenter: Option[String] = None,
    var adminPCid: Option[String] = None,
    var contactSource: Option[String] = None,
    var contactStatus: Option[String] = None,
    var notes: Option[String] = None,
    var properties: Option[JsonNode] = None,
    var createdTime: Option[Long] = None,
    var updatedTime: Option[Long] = None,
    var status: Option[Int] = None
) extends JdbcRecord {

  override def getPrimaryKeys(): Seq[String] = PContact.PRIMARY_IDS
  override def getFields(): Seq[String] = PContact.FIELDS

  override def setValues(field: String, value: Any): Unit = field match {
    case PContact.ID                => id = value.asOpt
    case PContact.P_CID             => pCid = value.asOpt
    case PContact.CID               => cid = value.asOpt
    case PContact.NICKNAME          => nickname = value.asOpt
    case PContact.FIRST_NAME        => firstName = value.asOpt
    case PContact.LAST_NAME         => lastName = value.asOpt
    case PContact.FULL_NAME         => fullName = value.asOpt
    case PContact.PHONE             => phone = value.asOpt
    case PContact.EMAIL             => email = value.asOpt
    case PContact.ADDITIONAL_EMAILS => additionalEmails = value.asOptString.map(JsonUtils.fromJson[Seq[String]])
    case PContact.AVATAR            => avatar = value.asOpt
    case PContact.ADMIN_P_CID       => adminPCid = value.asOpt
    case PContact.CONTACT_SOURCE    => contactSource = value.asOpt
    case PContact.CONTACT_STATUS    => contactStatus = value.asOpt
    case PContact.NOTES             => notes = value.asOpt
    case PContact.PROPERTIES =>
      properties = value.asOptString
        .map(JsonUtils.fromJson[JsonNode])
        .filterNot(_.isEmpty)
        .filterNot(_.isNull)
        .filterNot(_.isMissingNode)
    case PContact.OWNER        => owner = value.asOpt
    case PContact.OWNER_TEAM   => ownerTeam = value.asOpt
    case PContact.OWNER_MC     => ownerMarketCenter = value.asOpt
    case PContact.CREATED_TIME => createdTime = value.asOpt
    case PContact.UPDATED_TIME => updatedTime = value.asOpt
    case PContact.STATUS       => status = value.asOpt
    case _                     =>
  }

  override def getValue(field: String): Option[Any] = field match {
    case PContact.ID                => id
    case PContact.P_CID             => pCid
    case PContact.CID               => cid
    case PContact.NICKNAME          => nickname
    case PContact.FIRST_NAME        => firstName
    case PContact.LAST_NAME         => lastName
    case PContact.FULL_NAME         => fullName
    case PContact.PHONE             => phone
    case PContact.EMAIL             => email
    case PContact.ADDITIONAL_EMAILS => additionalEmails.map(JsonUtils.toJson(_, pretty = false))
    case PContact.AVATAR            => avatar
    case PContact.ADMIN_P_CID       => adminPCid
    case PContact.CONTACT_SOURCE    => contactSource
    case PContact.CONTACT_STATUS    => contactStatus
    case PContact.NOTES             => notes
    case PContact.PROPERTIES        => properties.map(JsonUtils.toJson(_, pretty = false))
    case PContact.OWNER             => owner
    case PContact.OWNER_TEAM        => ownerTeam
    case PContact.OWNER_MC          => ownerMarketCenter
    case PContact.CREATED_TIME      => createdTime
    case PContact.UPDATED_TIME      => updatedTime
    case PContact.STATUS            => status
    case _                          => throw SqlFieldMissing(field)
  }

  def toMap: Map[String, Any] = {
    PContact.FIELDS
      .map(field => field -> getValue(field).get)
      .toMap
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
