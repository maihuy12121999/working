package rever.etl.data_sync.jobs.contact

import rever.etl.data_sync.core.Normalizer
import rever.etl.data_sync.domain.GenericRecord
import rever.etl.data_sync.domain.contact.{SysContact, SysContactDM}
import rever.etl.data_sync.util.Utils

/** @author anhlt (andy)
  */
case class SysContactNormalizer() extends Normalizer[GenericRecord, Map[String, Any]] {

  override def toRecord(record: GenericRecord, total: Long): Option[Map[String, Any]] = {

    val sysContact = SysContact.fromGenericRecord(record)

    val personalTitle = sysContact.personalTitle

    val dataMap = Map[String, Any](
      SysContactDM.ID -> sysContact.id.get,
      SysContactDM.CID -> sysContact.cid.getOrElse(""),
      SysContactDM.PERSONAL_TITLE -> personalTitle.getOrElse(""),
      SysContactDM.FIRST_NAME -> sysContact.firstName.getOrElse(""),
      SysContactDM.LAST_NAME -> sysContact.lastName.getOrElse(""),
      SysContactDM.FULL_NAME -> sysContact.fullName.getOrElse(""),
      SysContactDM.GENDER -> Utils.computeGender(sysContact.gender, personalTitle).getOrElse(""),
      SysContactDM.PHONE -> sysContact.phone.getOrElse(""),
      SysContactDM.EMAILS -> sysContact.email.getOrElse(Seq.empty).toArray,
      SysContactDM.AVATAR -> sysContact.avatar.getOrElse(""),
      SysContactDM.CONTACT_SOURCE -> sysContact.contactSource.getOrElse(""),
      SysContactDM.C_CONTACT_SOURCE -> sysContact.cContactSource.getOrElse(""),
      SysContactDM.SEGMENT_TAGS -> sysContact.cSegmentTags.toArray,
      SysContactDM.HUBSPOT_IDS -> sysContact.hubspotIds.getOrElse(Seq.empty).toArray,
      SysContactDM.PROPERTIES -> sysContact.properties
        .filterNot(_.isNull)
        .filterNot(_.isEmpty)
        .filterNot(_.isMissingNode)
        .map(_.toString)
        .getOrElse("{}"),
      SysContactDM.CREATED_BY -> sysContact.createdBy.getOrElse(""),
      SysContactDM.UPDATED_BY -> sysContact.updatedBy.getOrElse(""),
      SysContactDM.CREATED_TIME -> sysContact.createdTime.getOrElse(0L),
      SysContactDM.UPDATED_TIME -> sysContact.updatedTime.getOrElse(0L),
      SysContactDM.STATUS -> sysContact.status.getOrElse(0),
      SysContactDM.DATA_HEALTH_SCORE -> 0.0,
      SysContactDM.DATA_HEALTH_INFO -> "{}",
      SysContactDM.LOG_TIME -> System.currentTimeMillis()
    )

    Some(dataMap)
  }

}
