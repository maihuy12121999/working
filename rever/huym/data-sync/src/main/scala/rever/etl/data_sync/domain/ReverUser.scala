package rever.etl.data_sync.domain

import com.fasterxml.jackson.databind.JsonNode
import rever.etl.data_sync.util.EmploymentStatusUtils
import rever.etl.data_sync.util.Utils.ImplicitAny
import rever.etl.rsparkflow.utils.JsonUtils
import vn.rever.common.util.JsonHelper
import vn.rever.jdbc.{JdbcRecord, SqlFieldMissing}

import scala.jdk.CollectionConverters.asScalaIteratorConverter

case class EmploymentStatus(
    status: String,
    effectiveDate: String,
    effectiveTime: Long,
    comment: Option[String]
)

object ReverUserDM {
  final val TBL_NAME = "user"

  final val ID = "id"
  final val USERNAME = "username"
  final val EMPLOYEE_ID = "employee_id"
  final val FIRST_NAME = "first_name"
  final val LAST_NAME = "last_name"
  final val FULL_NAME = "full_name"
  final val PHONE = "phone"
  final val PERSONAL_EMAIL = "personal_email"
  final val WORK_EMAIL = "work_email"
  final val AVATAR = "avatar"
  final val REPORT_TO = "job_report_to"
  final val HUBSPOT_ID = "hubspot_id"
  final val TEAM_ID = "team"
  final val MARKET_CENTER_ID = "market_center"
  final val OFFICE_ID = "office"

  final val PROPERTIES = "properties"
  final val CREATED_TIME = "created_time"
  final val UPDATED_TIME = "updated_time"
  final val STATUS = "status"

  // List of fields in the target datamart
  final val GENDER = "gender"
  final val BIRTHDAY = "birthday"
  final val USER_TYPE = "user_type"
  final val STAFF_TYPE = "staff_type"
  final val IS_AGENT = "is_agent"
  final val JOB_TITLE = "job_title"
  final val JOB_LOCATION = "job_location"
  final val REFERRAL_BY = "referral_by"
  final val MARITAL_STATUS = "marital_status"
  final val EMPLOYMENT_STATUS = "employment_status"
  final val BUSINESS_UNIT = "business_unit"
  final val LOG_TIME = "log_time"

  final val PRIMARY_IDS = Seq(ID)

  val FIELDS = Seq(
    ID,
    USERNAME,
    EMPLOYEE_ID,
    FIRST_NAME,
    LAST_NAME,
    FULL_NAME,
    USER_TYPE,
    STAFF_TYPE,
    IS_AGENT,
    GENDER,
    BIRTHDAY,
    JOB_TITLE,
    JOB_LOCATION,
    REFERRAL_BY,
    MARITAL_STATUS,
    EMPLOYMENT_STATUS,
    PHONE,
    PERSONAL_EMAIL,
    WORK_EMAIL,
    AVATAR,
    REPORT_TO,
    HUBSPOT_ID,
    TEAM_ID,
    MARKET_CENTER_ID,
    BUSINESS_UNIT,
    OFFICE_ID,
    PROPERTIES,
    UPDATED_TIME,
    CREATED_TIME,
    STATUS,
    LOG_TIME
  )

}

object ReverUser {
  final val TBL_NAME = "user"

  final val ID = "id"
  final val USERNAME = "username"
  final val EMPLOYEE_ID = "employee_id"
  final val FIRST_NAME = "first_name"
  final val LAST_NAME = "last_name"
  final val FULL_NAME = "full_name"
  final val PHONE = "phone"
  final val PERSONAL_EMAIL = "personal_email"
  final val WORK_EMAIL = "work_email"
  final val AVATAR = "avatar"
  final val REPORT_TO = "job_report_to"
  final val HUBSPOT_ID = "hubspot_id"
  final val TEAM_ID = "team"
  final val MARKET_CENTER_ID = "market_center"
  final val OFFICE_ID = "office"

  final val PROPERTIES = "properties"
  final val CREATED_TIME = "created_time"
  final val UPDATED_TIME = "updated_time"
  final val STATUS = "status"

  // List of fields in the target datamart
  final val GENDER = "gender"
  final val BIRTHDAY = "birthday"
  final val USER_TYPE = "user_type"
  final val IS_AGENT = "is_agent"
  final val JOB_TITLE = "job_title"
  final val JOB_LOCATION = "job_location"
  final val REFERRAL_BY = "referral_by"
  final val MARITAL_STATUS = "marital_status"
  final val EMPLOYMENT_STATUS = "employment_status"
  final val BUSINESS_UNIT = "business_unit"

  final val PRIMARY_IDS = Seq(ID)

  val FIELDS = Seq(
    ID,
    USERNAME,
    EMPLOYEE_ID,
    FIRST_NAME,
    LAST_NAME,
    FULL_NAME,
    PHONE,
    PERSONAL_EMAIL,
    WORK_EMAIL,
    AVATAR,
    REPORT_TO,
    HUBSPOT_ID,
    TEAM_ID,
    MARKET_CENTER_ID,
    OFFICE_ID,
    PROPERTIES,
    UPDATED_TIME,
    CREATED_TIME,
    STATUS
  )

  final def fromGenericRecord(record: GenericRecord): ReverUser = {
    val contact = ReverUser()
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

case class ReverUser(
    var id: Option[Long] = None,
    var username: Option[String] = None,
    var employeeId: Option[String] = None,
    var firstName: Option[String] = None,
    var lastName: Option[String] = None,
    var fullName: Option[String] = None,
    var phone: Option[String] = None,
    var personalEmail: Option[Seq[String]] = None,
    var workEmail: Option[Seq[String]] = None,
    var avatar: Option[String] = None,
    var jobReportTo: Option[String] = None,
    var hubspotId: Option[String] = None,
    var teamId: Option[String] = None,
    var marketCenterId: Option[String] = None,
    var officeId: Option[String] = None,
    var properties: Option[JsonNode] = None,
    var createdTime: Option[Long] = None,
    var updatedTime: Option[Long] = None,
    var status: Option[Int] = None
) extends JdbcRecord {

  override def getPrimaryKeys(): Seq[String] = ReverUser.PRIMARY_IDS
  override def getFields(): Seq[String] = ReverUser.FIELDS

  override def setValues(field: String, value: Any): Unit = field match {
    case ReverUser.ID               => id = value.asOpt
    case ReverUser.USERNAME         => username = value.asOpt
    case ReverUser.EMPLOYEE_ID      => employeeId = value.asOpt
    case ReverUser.FIRST_NAME       => firstName = value.asOpt
    case ReverUser.LAST_NAME        => lastName = value.asOpt
    case ReverUser.FULL_NAME        => fullName = value.asOpt
    case ReverUser.PHONE            => phone = value.asOpt
    case ReverUser.PERSONAL_EMAIL   => personalEmail = value.asOpt
    case ReverUser.WORK_EMAIL       => workEmail = value.asOpt
    case ReverUser.AVATAR           => avatar = value.asOpt
    case ReverUser.REPORT_TO        => jobReportTo = value.asOpt
    case ReverUser.HUBSPOT_ID       => hubspotId = value.asOpt
    case ReverUser.TEAM_ID          => teamId = value.asOpt
    case ReverUser.MARKET_CENTER_ID => marketCenterId = value.asOpt
    case ReverUser.OFFICE_ID        => officeId = value.asOpt
    case ReverUser.PROPERTIES =>
      properties = value.asOptString
        .map(JsonUtils.fromJson[JsonNode])
        .filterNot(_.isEmpty)
        .filterNot(_.isNull)
        .filterNot(_.isMissingNode)
    case ReverUser.CREATED_TIME => createdTime = value.asOpt
    case ReverUser.UPDATED_TIME => updatedTime = value.asOpt
    case ReverUser.STATUS       => status = value.asOpt
    case _                      =>
  }

  override def getValue(field: String): Option[Any] = field match {
    case ReverUser.ID               => id
    case ReverUser.USERNAME         => username
    case ReverUser.EMPLOYEE_ID      => employeeId
    case ReverUser.FIRST_NAME       => firstName
    case ReverUser.LAST_NAME        => lastName
    case ReverUser.FULL_NAME        => fullName
    case ReverUser.PHONE            => phone
    case ReverUser.PERSONAL_EMAIL   => personalEmail
    case ReverUser.WORK_EMAIL       => workEmail
    case ReverUser.AVATAR           => avatar
    case ReverUser.REPORT_TO        => jobReportTo
    case ReverUser.HUBSPOT_ID       => hubspotId
    case ReverUser.TEAM_ID          => teamId
    case ReverUser.MARKET_CENTER_ID => marketCenterId
    case ReverUser.OFFICE_ID        => officeId
    case ReverUser.PROPERTIES =>
      properties.map(_.toString)
    case ReverUser.CREATED_TIME => createdTime
    case ReverUser.UPDATED_TIME => updatedTime
    case ReverUser.STATUS       => status
    case _                      => throw SqlFieldMissing(field)
  }

  def businessUnit: Option[String] = {
    getStringField("business_unit").map {
      case bu if bu.contains("primary")   => "primary"
      case bu if bu.contains("secondary") => "secondary"
      case bu                             => bu.toLowerCase()
    }
  }

  def getStringField(field: String): Option[String] = {
    properties
      .map(_.at(s"/${field}").asText(""))
      .filterNot(_ == null)
      .filterNot(_.isEmpty)
  }

  def employmentStatuses: Seq[JsonNode] = {
    val statuses = properties
      .map(_.at("/employment_status"))
      .filterNot(_.isNull)
      .filterNot(_.isEmpty)
      .filterNot(_.isMissingNode)
      .filter(_.isArray)
      .map(_.elements().asScala.toSeq)
      .getOrElse(Seq.empty)
      .map(EmploymentStatusUtils.fromJsonNode)

    statuses
      .map(JsonHelper.toJson(_, pretty = false))
      .map(JsonHelper.fromJson[JsonNode](_))
  }

  def toMap: Map[String, Any] = {
    ReverUser.FIELDS
      .map(field => field -> getValue(field).get)
      .toMap
  }

}
