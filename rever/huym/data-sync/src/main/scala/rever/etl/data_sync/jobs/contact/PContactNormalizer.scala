package rever.etl.data_sync.jobs.contact

import rever.data_health.domain.p_contact.PContactHealthItem
import rever.data_health.domain.{ContactHealthEvaluator, DataHealthScore}
import rever.etl.data_sync.core.Normalizer
import rever.etl.data_sync.domain.GenericRecord
import rever.etl.data_sync.domain.contact.{PContact, PContactDM}
import rever.etl.data_sync.util.Utils
import rever.etl.rsparkflow.client.PushKafkaClient
import rever.etl.rsparkflow.domain.data_delivery.DataDeliveryMsg
import vn.rever.common.util.JsonHelper

/** @author anhlt (andy)
  */
case class PContactNormalizer(
    contactHealthEvaluator: ContactHealthEvaluator,
    isPutKafka: Boolean,
    pushKafkaClient: PushKafkaClient,
    pContactEnrichmentTopic: String
) extends Normalizer[GenericRecord, Map[String, Any]] {

  override def toRecord(record: GenericRecord, total: Long): Option[Map[String, Any]] = {

    val pContact = PContact.fromGenericRecord(record)
    val personalTitle = pContact.personalTitle

    val pCid = pContact.pCid.get

    val dataMap = Map[String, Any](
      PContactDM.ID -> pContact.id.get,
      PContactDM.P_CID -> pCid,
      PContactDM.CID -> pContact.cid.getOrElse(""),
      PContactDM.NICKNAME -> pContact.nickname.getOrElse(""),
      PContactDM.PERSONAL_TITLE -> pContact.personalTitle.getOrElse(""),
      PContactDM.FIRST_NAME -> pContact.firstName.getOrElse(""),
      PContactDM.LAST_NAME -> pContact.lastName.getOrElse(""),
      PContactDM.FULL_NAME -> pContact.fullName.getOrElse(""),
      PContactDM.GENDER -> Utils.computeGender(pContact.gender, personalTitle).getOrElse(""),
      PContactDM.PHONE -> pContact.phone.getOrElse(""),
      PContactDM.EMAIL -> pContact.email.getOrElse(""),
      PContactDM.ADDITIONAL_EMAILS -> pContact.additionalEmails.getOrElse(Seq.empty).toArray,
      PContactDM.AVATAR -> pContact.avatar.getOrElse(""),
      PContactDM.OWNER_MC -> pContact.ownerMarketCenter.getOrElse(""),
      PContactDM.OWNER_TEAM -> pContact.ownerTeam.getOrElse(""),
      PContactDM.OWNER -> pContact.owner.getOrElse(""),
      PContactDM.ADMIN_P_CID -> pContact.adminPCid.getOrElse(""),
      PContactDM.CONTACT_SOURCE -> pContact.contactSource.getOrElse(""),
      PContactDM.CONTACT_STATUS -> pContact.contactStatus.getOrElse(""),
      PContactDM.NOTES -> pContact.notes.getOrElse(""),
      PContactDM.PROPERTIES -> pContact.properties
        .filterNot(_.isNull)
        .filterNot(_.isEmpty)
        .filterNot(_.isMissingNode)
        .map(_.toString)
        .getOrElse("{}"),
      PContactDM.CREATED_TIME -> pContact.createdTime.getOrElse(0L),
      PContactDM.UPDATED_TIME -> pContact.updatedTime.getOrElse(0L),
      PContactDM.STATUS -> pContact.status.getOrElse(0),
      PContactDM.LOG_TIME -> System.currentTimeMillis()
    )

    val dataHealthScore = contactHealthEvaluator.evaluate(PContactHealthItem.fromPContactDM(dataMap))

    if (isPutKafka) {
      pushKafkaClient.multiDeliverDataToKafka(
        pContactEnrichmentTopic,
        Seq(toPContactDeliveryMsg(pCid, dataHealthScore))
      )
    }

    Some(
      dataMap ++ Map(
        PContactDM.DATA_HEALTH_SCORE -> dataHealthScore.score,
        PContactDM.DATA_HEALTH_INFO -> JsonHelper.toJson(dataHealthScore.fields, pretty = false)
      )
    )
  }

  private def toPContactDeliveryMsg(pCid: String, dataHealthScore: DataHealthScore): DataDeliveryMsg = {
    DataDeliveryMsg(
      objectType = "personal_contact",
      objectId = pCid,
      replaceFields = Some(
        Map[String, Any](
          PContactDM.DATA_HEALTH_SCORE -> dataHealthScore.score,
          PContactDM.DATA_HEALTH_INFO -> JsonHelper.toJson(dataHealthScore.fields, pretty = false)
        )
      ),
      appendFields = None,
      removeFields = None,
      deleteFields = None,
      timestamp = System.currentTimeMillis(),
      dataService = "data_health_service"
    )
  }

}
