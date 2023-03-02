package rever.etl.data_sync.normalizer

import rever.etl.data_sync.core.Normalizer
import rever.etl.data_sync.domain.{GenericRecord, ReverUser, ReverUserDM}

/** @author anhlt (andy)
  */
case class ReverUserNormalizer(
    agentJobTitles: Set[String],
    smJobTitles: Set[String],
    sdJobTitles: Set[String],
    systemUserIds: Set[String]
) extends Normalizer[GenericRecord, Map[String, Any]] {

  override def toRecord(record: GenericRecord, total: Long): Option[Map[String, Any]] = {

    val user = ReverUser.fromGenericRecord(record)
    val username = user.username.getOrElse("")
    val jobTitle = user.getStringField("job_title").getOrElse("")

    val dataMap = Map[String, Any](
      ReverUserDM.ID -> user.id.get,
      ReverUserDM.USERNAME -> username,
      ReverUserDM.EMPLOYEE_ID -> user.employeeId.getOrElse(""),
      ReverUserDM.FIRST_NAME -> user.firstName.getOrElse(""),
      ReverUserDM.LAST_NAME -> user.lastName.getOrElse(""),
      ReverUserDM.FULL_NAME -> user.fullName.getOrElse(""),
      ReverUserDM.USER_TYPE -> user.getStringField("user_type").getOrElse(""),
      ReverUserDM.STAFF_TYPE -> calcStaffType(username, jobTitle),
      ReverUserDM.IS_AGENT -> calcIsAgent(username, jobTitle),
      ReverUserDM.GENDER -> user.getStringField("gender").getOrElse(""),
      ReverUserDM.BIRTHDAY -> user.getStringField("birthday").getOrElse(""),
      ReverUserDM.JOB_TITLE -> jobTitle,
      ReverUserDM.JOB_LOCATION -> user.getStringField("job_location").getOrElse(""),
      ReverUserDM.REFERRAL_BY -> user.getStringField("referral_by").getOrElse(""),
      ReverUserDM.MARITAL_STATUS -> user.getStringField("marital_status").getOrElse(""),
      ReverUserDM.EMPLOYMENT_STATUS -> user.employmentStatuses.map(_.toString).toArray,
      ReverUserDM.PHONE -> user.phone.getOrElse(""),
      ReverUserDM.PERSONAL_EMAIL -> user.personalEmail.getOrElse(""),
      ReverUserDM.WORK_EMAIL -> user.workEmail.getOrElse(""),
      ReverUserDM.AVATAR -> user.avatar.getOrElse(""),
      ReverUserDM.REPORT_TO -> user.jobReportTo.getOrElse(""),
      ReverUserDM.HUBSPOT_ID -> user.hubspotId.getOrElse(""),
      ReverUserDM.TEAM_ID -> user.teamId.getOrElse(""),
      ReverUserDM.MARKET_CENTER_ID -> user.marketCenterId.getOrElse(""),
      ReverUserDM.BUSINESS_UNIT -> user.businessUnit.getOrElse(""),
      ReverUserDM.OFFICE_ID -> user.officeId.getOrElse(""),
      ReverUserDM.PROPERTIES -> user.properties
        .filterNot(_.isNull)
        .filterNot(_.isEmpty)
        .filterNot(_.isMissingNode)
        .map(_.toString)
        .getOrElse("{}"),
      ReverUserDM.CREATED_TIME -> user.createdTime.getOrElse(0L),
      ReverUserDM.UPDATED_TIME -> user.updatedTime.getOrElse(0L),
      ReverUserDM.STATUS -> user.status.getOrElse(0),
      ReverUserDM.LOG_TIME -> System.currentTimeMillis()
    )
    Some(dataMap)
  }

  private def calcIsAgent(username: String, jobTitle: String): Boolean = {
    agentJobTitles.contains(jobTitle) && !systemUserIds.contains(username)
  }

  private def calcStaffType(username: String, jobTitle: String): String = {
    systemUserIds.contains(username) match {
      case true                                   => ""
      case _ if agentJobTitles.contains(jobTitle) => "rva"
      case _ if smJobTitles.contains(jobTitle)    => "sm"
      case _ if sdJobTitles.contains(jobTitle)    => "sd"
      case _                                      => ""
    }
  }

}
