package rever.etl.data_sync.normalizer

import rever.etl.data_sync.core.Normalizer
import rever.etl.data_sync.domain.{GenericRecord, Opportunity, OpportunityDM}

/** @author anhlt (andy)
  */
case class GenericOppoNormalizer() extends Normalizer[GenericRecord, Map[String, Any]] {

  override def toRecord(record: GenericRecord, total: Long): Option[Map[String, Any]] = {

    val opportunity = Opportunity.fromGenericRecord(record)

    val dataMap = Map[String, Any](
      "id" -> opportunity.id.get,
      "pipeline_version" -> opportunity.pipelineVersion.getOrElse(0),
      "pipeline_id" -> opportunity.pipelineId.get,
      "phase_id" -> opportunity.phaseId.get,
      "stage_id" -> opportunity.stageId.get,
      "name" -> opportunity.name.getOrElse(""),
      "position" -> opportunity.position.getOrElse(0.0),
      "service_type" -> opportunity.serviceType.getOrElse(""),
      "transaction_type" -> opportunity.transactionType.getOrElse(""),
      "estimate_price" -> opportunity.estimatePrice.getOrElse(0.0),
      "commission_type" -> opportunity.commissionType.getOrElse(""),
      "commission_value" -> opportunity.commissionValue.getOrElse(0.0),
      "commission_price" -> opportunity.commissionPrice.getOrElse(0.0),
      "booking_value" -> opportunity.bookingValue.getOrElse(0.0),
      "deposit_value" -> opportunity.depositValue.getOrElse(0.0),
      "closed_status" -> opportunity.closedStatus
        .filterNot(_ == null)
        .filterNot(_.isEmpty)
        .getOrElse("open"),
      "closed_date" -> opportunity.closedDate.getOrElse(0L),
      "due_date" -> opportunity.dueDate.getOrElse(0L),
      "closed_by" -> opportunity.closedBy.getOrElse(""),
      "stage_start_time" -> opportunity.stageStatusStartTime.getOrElse(0L),
      "stage_status" -> opportunity.stageStatus.getOrElse(100),
      "stage_status_start_time" -> opportunity.stageStatusStartTime.getOrElse(0L),
      "p_mc_ids" -> opportunity.pMcIds.getOrElse(Seq.empty).toArray,
      "p_team_ids" -> opportunity.pTeamIds.getOrElse(Seq.empty).toArray,
      "p_user_ids" -> opportunity.pUserIds.getOrElse(Seq.empty).toArray,
      "customer_identities" -> opportunity.customerIdentities.map(_.toString).getOrElse("{}"),
      "contact_source" -> opportunity.contactSource.getOrElse(""),
      "customer_name" -> opportunity.customerName.getOrElse(""),
      "customer_id" -> opportunity.customerId.getOrElse(""),
      "customer_cid" -> opportunity.customerCid.getOrElse(""),
      "user_related_ids" -> opportunity.userRelatedIds
        .map(_.filterNot(_ == null).filterNot(_.isEmpty))
        .getOrElse(Seq.empty)
        .toArray,
      "area_ids" -> opportunity.areaIds
        .map(_.filterNot(_ == null).filterNot(_.isEmpty))
        .getOrElse(Seq.empty)
        .toArray,
      "neighborhood_ids" -> opportunity.neighborhoodIds
        .map(_.filterNot(_ == null).filterNot(_.isEmpty))
        .getOrElse(Seq.empty)
        .toArray,
      "project_ids" -> opportunity.projectIds
        .map(_.filterNot(_ == null).filterNot(_.isEmpty))
        .getOrElse(Seq.empty)
        .toArray,
      "project_id" -> opportunity.projectId.getOrElse(""),
      "listing_type" -> opportunity.listingType.getOrElse(""),
      "property_id" -> opportunity.propertyId.getOrElse(""),
      "property_info" -> opportunity.propertyInfo.map(_.toString).getOrElse("{}"),
      "inquiry_id" -> opportunity.inquiryId.getOrElse(""),
      "location_name" -> opportunity.locationName.getOrElse(""),
      "locations" -> opportunity.locations.map(_.map(_.toString)).getOrElse(Seq.empty).toArray,
      "mc_id" -> opportunity.mcId.getOrElse(""),
      "owner_mc" -> opportunity.ownerMc.getOrElse(""),
      "owner_team" -> opportunity.ownerTeam.getOrElse(""),
      "owner" -> opportunity.owner.getOrElse(""),
      "additional_infos" -> opportunity.additionalInfo.map(_.toString).getOrElse("{}"),
      "owner_distributed_job_id" -> opportunity.ownerDistributedJobId.getOrElse(""),
      "affiliate_pcid" -> opportunity.affiliatePcid.getOrElse(""),
      "affiliate_cid" -> opportunity.affiliateCid.getOrElse(""),
      "purposes" -> opportunity.purposes.getOrElse(Seq.empty[String]).toArray,
      "priority" -> opportunity.priority.getOrElse(0),
      "created_time" -> opportunity.createdTime.getOrElse(0L),
      "updated_time" -> opportunity.updatedTime.getOrElse(0L),
      "created_by" -> opportunity.createdBy.getOrElse(""),
      "updated_by" -> opportunity.updatedBy.getOrElse(""),
      "status" -> opportunity.status.getOrElse(1),
      "tag_ids" -> opportunity.tagIds
        .map(_.filterNot(_ == null))
        .getOrElse(Seq.empty)
        .toArray,
      "note" -> opportunity.note.getOrElse(""),
      "process_tasks_done" -> opportunity.processTasksDone
        .map(_.filterNot(_ == null))
        .getOrElse(Seq.empty)
        .toArray,
      OpportunityDM.LOG_TIME -> System.currentTimeMillis()
    )
    Some(dataMap)
  }

}
case class OppoNormalizer() extends Normalizer[Opportunity, Map[String, Any]] {

  override def toRecord(opportunity: Opportunity, total: Long): Option[Map[String, Any]] = {

    val dataMap = Map[String, Any](
      "id" -> opportunity.id.get,
      "pipeline_version" -> opportunity.pipelineVersion.getOrElse(0),
      "pipeline_id" -> opportunity.pipelineId.get,
      "phase_id" -> opportunity.phaseId.get,
      "stage_id" -> opportunity.stageId.get,
      "name" -> opportunity.name.getOrElse(""),
      "position" -> opportunity.position.getOrElse(0.0),
      "service_type" -> opportunity.serviceType.getOrElse(""),
      "transaction_type" -> opportunity.transactionType.getOrElse(""),
      "estimate_price" -> opportunity.estimatePrice.getOrElse(0.0),
      "commission_type" -> opportunity.commissionType.getOrElse(""),
      "commission_value" -> opportunity.commissionValue.getOrElse(0.0),
      "commission_price" -> opportunity.commissionPrice.getOrElse(0.0),
      "booking_value" -> opportunity.bookingValue.getOrElse(0.0),
      "deposit_value" -> opportunity.depositValue.getOrElse(0.0),
      "closed_status" -> opportunity.closedStatus.getOrElse(""),
      "closed_date" -> opportunity.closedDate.getOrElse(0L),
      "due_date" -> opportunity.dueDate.getOrElse(0L),
      "closed_by" -> opportunity.closedBy.getOrElse(""),
      "stage_start_time" -> opportunity.stageStatusStartTime.getOrElse(0L),
      "stage_status" -> opportunity.stageStatus.getOrElse(100),
      "stage_status_start_time" -> opportunity.stageStatusStartTime.getOrElse(0L),
      "p_mc_ids" -> opportunity.pMcIds.getOrElse(Seq.empty).toArray,
      "p_team_ids" -> opportunity.pTeamIds.getOrElse(Seq.empty).toArray,
      "p_user_ids" -> opportunity.pUserIds.getOrElse(Seq.empty).toArray,
      "customer_identities" -> opportunity.customerIdentities.map(_.toString).getOrElse("{}"),
      "contact_source" -> opportunity.contactSource.getOrElse(""),
      "customer_name" -> opportunity.customerName.getOrElse(""),
      "customer_id" -> opportunity.customerId.getOrElse(""),
      "customer_cid" -> opportunity.customerCid.getOrElse(""),
      "user_related_ids" -> opportunity.userRelatedIds
        .map(_.filterNot(_ == null).filterNot(_.isEmpty))
        .getOrElse(Seq.empty)
        .toArray,
      "area_ids" -> opportunity.areaIds
        .map(_.filterNot(_ == null).filterNot(_.isEmpty))
        .getOrElse(Seq.empty)
        .toArray,
      "neighborhood_ids" -> opportunity.neighborhoodIds
        .map(_.filterNot(_ == null).filterNot(_.isEmpty))
        .getOrElse(Seq.empty)
        .toArray,
      "project_ids" -> opportunity.projectIds
        .map(_.filterNot(_ == null).filterNot(_.isEmpty))
        .getOrElse(Seq.empty)
        .toArray,
      "project_id" -> opportunity.projectId.getOrElse(""),
      "listing_type" -> opportunity.listingType.getOrElse(""),
      "property_id" -> opportunity.propertyId.getOrElse(""),
      "property_info" -> opportunity.propertyInfo.map(_.toString).getOrElse("{}"),
      "inquiry_id" -> opportunity.inquiryId.getOrElse(""),
      "location_name" -> opportunity.locationName.getOrElse(""),
      "locations" -> opportunity.locations.map(_.map(_.toString)).getOrElse(Seq.empty).toArray,
      "mc_id" -> opportunity.mcId.getOrElse(""),
      "owner_mc" -> opportunity.ownerMc.getOrElse(""),
      "owner_team" -> opportunity.ownerTeam.getOrElse(""),
      "owner" -> opportunity.owner.getOrElse(""),
      "additional_infos" -> opportunity.additionalInfo.map(_.toString).getOrElse("{}"),
      "owner_distributed_job_id" -> opportunity.ownerDistributedJobId.getOrElse(""),
      "affiliate_pcid" -> opportunity.affiliatePcid.getOrElse(""),
      "affiliate_cid" -> opportunity.affiliateCid.getOrElse(""),
      "purposes" -> opportunity.purposes.getOrElse(Seq.empty[String]).toArray,
      "priority" -> opportunity.priority.getOrElse(0),
      "created_time" -> opportunity.createdTime.getOrElse(0L),
      "updated_time" -> opportunity.updatedTime.getOrElse(0L),
      "created_by" -> opportunity.createdBy.getOrElse(""),
      "updated_by" -> opportunity.updatedBy.getOrElse(""),
      "status" -> opportunity.status.getOrElse(1),
      "tag_ids" -> opportunity.tagIds
        .map(_.filterNot(_ == null))
        .getOrElse(Seq.empty)
        .toArray,
      "note" -> opportunity.note.getOrElse(""),
      "process_tasks_done" -> opportunity.processTasksDone
        .map(_.filterNot(_ == null))
        .getOrElse(Seq.empty)
        .toArray
    )
    Some(dataMap)
  }

}
