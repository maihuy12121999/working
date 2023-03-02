package rever.etl.data_sync.jobs.call

import org.elasticsearch.search.SearchHit
import rever.etl.data_sync.core.Normalizer
import rever.etl.data_sync.domain.{CallHistory, CallHistoryDM}
import rever.etl.data_sync.jobs.call.CallHistoryNormalizer.getNormalizedPhoneFromMap
import rever.etl.data_sync.util.NormalizedPhoneUtils
import rever.etl.data_sync.util.NormalizedPhoneUtils._
import rever.etl.rsparkflow.api.configuration.Config
import rever.etl.rsparkflow.client.DataMappingClient
import rever.etl.rsparkflow.utils.JsonUtils

import scala.collection.mutable

/** @author anhlt (andy)
  */
case class CallHistoryNormalizer(config: Config) extends Normalizer[SearchHit, Map[String, Any]] {

  private lazy val mappingClient = DataMappingClient.client(config)

  override def toRecord(searchHit: SearchHit, total: Long): Option[Map[String, Any]] = {

    val callId = searchHit.getId
    val call = JsonUtils.fromJson[CallHistory](searchHit.getSourceAsString)

    val source = call.source.getOrElse("")
    val destination = call.destination.getOrElse("")

    val agentId = resolveAgentId(call)

    val dataMap = Map(
      CallHistoryDM.CALL_ID -> callId,
      CallHistoryDM.DIRECTION -> call.direction.getOrElse(""),
      CallHistoryDM.SOURCE -> source,
      CallHistoryDM.DESTINATION -> destination,
      CallHistoryDM.NORMALIZED_SOURCE -> getNormalizedPhoneFromMap(NormalizedPhoneUtils.normalizeNumberPhone(source)),
      CallHistoryDM.NORMALIZED_DESTINATION -> getNormalizedPhoneFromMap(NormalizedPhoneUtils.normalizeNumberPhone(destination)),
      CallHistoryDM.CALL_SERVICE -> call.callService.getOrElse(""),
      CallHistoryDM.EXT_SERVICE_CALL_ID -> call.extServiceCallId.getOrElse(""),
      CallHistoryDM.INTERNAL_NUMBER -> call.internalNumber.getOrElse(""),
      CallHistoryDM.GROUP -> call.group.getOrElse(""),
      CallHistoryDM.IVR -> call.ivr.getOrElse(""),
      CallHistoryDM.QUEUE -> call.queue.getOrElse(""),
      CallHistoryDM.TTA -> call.tta.getOrElse(0),
      CallHistoryDM.DURATION -> call.duration.getOrElse(0),
      CallHistoryDM.RECORDING_FILE -> call.recordingFile.getOrElse(""),
      CallHistoryDM.SIP -> call.sip.map(_.toString).getOrElse("{}"),
      CallHistoryDM.CID -> call.cid.getOrElse(""),
      CallHistoryDM.P_CID -> "",
      CallHistoryDM.RAW_CONTACT_PHONE -> call.rawContactPhone.getOrElse(""),
      CallHistoryDM.CONTACT -> call.contact.map(_.toString).getOrElse("{}"),
      CallHistoryDM.MARKET_CENTER_ID -> call.agentMarketCenterId.getOrElse(""),
      CallHistoryDM.TEAM_ID -> call.agentTeamId.getOrElse(""),
      CallHistoryDM.AGENT_ID -> agentId.getOrElse(""),
      CallHistoryDM.AGENT -> call.agent.map(_.toString).getOrElse("{}"),
      CallHistoryDM.ADDITIONAL_INFO -> call.additionalInfo.map(_.toString).getOrElse("{}"),
      CallHistoryDM.CLIENT_INFO -> call.clientInfo.map(_.toString).getOrElse("{}"),
      CallHistoryDM.TIME -> call.time.getOrElse(0L),
      CallHistoryDM.CREATED_TIME -> call.createdTime.getOrElse(0L),
      CallHistoryDM.UPDATED_TIME -> call.updatedTime.getOrElse(0L),
      CallHistoryDM.STATUS -> call.status.getOrElse(""),
      CallHistoryDM.LOG_TIME -> System.currentTimeMillis()
    )

    Some(dataMap)
  }

  /** Resolve agent id for the given call history, there are 3 steps <br>
    * For now, only the 1st method is applied
    * + Resolve from Agent object: email -> agent id
    * + Source/ destination (Call Service) -> agent id
    *  + Personal phone number -> agent id
    */
  private def resolveAgentId(call: CallHistory): Option[String] = {
    val emailOpt = call.agentEmail
    call.agentId match {
      case Some(agentId) => Some(agentId)
      case None if emailOpt.isDefined =>
        val email = emailOpt.get
        mappingClient.mGetUserByEmail(Seq(email)).get(email).map(_.username)
      case _ => None
    }
  }


}

object CallHistoryNormalizer{
  def getNormalizedPhoneFromMap(phonesMap:mutable.Map[String,String]):String={
    val priorityRegions= priorityRegionCodes
      .filter(phonesMap.contains)

    priorityRegions match {
      case regions if(regions.nonEmpty) => phonesMap.getOrElse(regions.head,"")
      case regions if(regions.isEmpty)=>{
        val nonPriorityRegions = nonPriorityRegionCodes.filter(phonesMap.contains)
        if(nonPriorityRegions.nonEmpty) phonesMap.getOrElse(nonPriorityRegions.head,"")
        else ""
      }
    }
  }
}
