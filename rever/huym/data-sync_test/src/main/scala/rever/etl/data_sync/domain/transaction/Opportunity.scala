package rever.etl.data_sync.domain.transaction

import com.fasterxml.jackson.databind.JsonNode
import rever.etl.data_sync.util.Utils.ImplicitAny
import rever.etl.rsparkflow.utils.JsonUtils
import vn.rever.jdbc.{JdbcRecord, SqlFieldMissing}

object Opportunity {
  val TBL_NAME = "opportunities"

  val ID = "id"

  val PIPELINE_VERSION = "pipeline_version"
  val PIPELINE_ID = "pipeline_id"
  val PHASE_ID = "phase_id"
  val STAGE_ID = "stage_id"
  val NAME = "name"
  val POSITION = "position"

  val SERVICE_TYPE = "service_type"
  val TRANSACTION_TYPE = "transaction_type"

  val ESTIMATE_PRICE = "estimate_price"
  val COMMISSION_TYPE = "commission_type"
  val COMMISSION_VALUE = "commission_value"
  val COMMISSION_PRICE = "commission_price"
  val BOOKING_VALUE = "booking_value"
  val DEPOSIT_VALUE = "deposit_value"
  val CLOSED_STATUS = "closed_status"
  val DUE_DATE = "due_date"
  val CLOSED_DATE = "closed_date"
  val CLOSED_BY = "closed_by"

  val STAGE_START_TIME = "stage_start_time"
  val STAGE_STATUS = "stage_status"
  val STAGE_STATUS_START_TIME = "stage_status_start_time"

  val PERMISSION_USER_IDS = "p_user_ids"
  val PERMISSION_TEAM_IDS = "p_team_ids"
  val PERMISSION_MC_IDS = "p_mc_ids"

  val CUSTOMER_IDENTITIES = "customer_identities"
  val CONTACT_SOURCE = "contact_source"
  val CUSTOMER_NAME = "customer_name"
  val CUSTOMER_ID = "customer_id"
  val CUSTOMER_CID = "customer_cid"
  val USER_RELATED_IDS = "user_related_ids"
  val AREA_IDS = "area_ids"
  val NEIGHBORHOOD_IDS = "neighborhood_ids"
  val PROJECT_IDS = "project_ids"
  val PROJECT_ID = "project_id"
  val LISTING_TYPE = "listing_type"
  val PROPERTY_ID = "property_id"
  val PROPERTY_INFO = "property_info"

  val INQUIRY_ID = "inquiry_id"
  val LOCATION_NAME = "location_name"
  val LOCATIONS = "locations"
  val OWNER = "owner"
  val MC_ID = "mc_id"
  val OWNER_MC = "owner_mc"
  val OWNER_TEAM = "owner_team"
  val PROCESS_TASKS_DONE = "process_tasks_done"

  val ADDITIONAL_INFO = "additional_infos"

  val OWNER_DISTRIBUTED_JOB_ID = "owner_distributed_job_id"

  val AFFILIATE_PCID = "affiliate_pcid"
  val AFFILIATE_CID = "affiliate_cid"

  val PURPOSES = "purposes"
  val PRIORITY = "priority"

  val CREATED_TIME = "created_time"
  val UPDATED_TIME = "updated_time"
  val CREATED_BY = "created_by"
  val UPDATED_BY = "updated_by"
  val STATUS = "status"
  val TAG_IDS = "tag_ids"
  val NOTE = "note"

  final val PRIMARY_IDS = Seq(ID)

  val FIELDS = Seq(
    ID,
    UPDATED_TIME,
    CREATED_TIME,
    CREATED_BY,
    UPDATED_BY,
    STATUS,
    POSITION,
    CUSTOMER_NAME,
    CLOSED_BY,
    PROJECT_IDS,
    AREA_IDS,
    OWNER_DISTRIBUTED_JOB_ID,
    ADDITIONAL_INFO,
    CONTACT_SOURCE,
    PIPELINE_ID,
    PHASE_ID,
    STAGE_ID,
    NAME,
    CUSTOMER_ID,
    NEIGHBORHOOD_IDS,
    PROJECT_ID,
    ESTIMATE_PRICE,
    COMMISSION_TYPE,
    COMMISSION_VALUE,
    DUE_DATE,
    CLOSED_DATE,
    STAGE_START_TIME,
    NOTE,
    PERMISSION_USER_IDS,
    PERMISSION_TEAM_IDS,
    PERMISSION_MC_IDS,
    PROPERTY_ID,
    COMMISSION_PRICE,
    CLOSED_STATUS,
    LOCATION_NAME,
    OWNER,
    MC_ID,
    OWNER_TEAM,
    OWNER_MC,
    PROCESS_TASKS_DONE,
    LOCATIONS,
    TAG_IDS,
    CUSTOMER_CID,
    USER_RELATED_IDS,
    SERVICE_TYPE,
    PROPERTY_INFO,
    TRANSACTION_TYPE,
    AFFILIATE_PCID,
    AFFILIATE_CID,
    BOOKING_VALUE,
    DEPOSIT_VALUE,
    LISTING_TYPE,
    INQUIRY_ID,
    PURPOSES,
    CUSTOMER_IDENTITIES,
    PRIORITY,
    STAGE_STATUS,
    PIPELINE_VERSION,
    STAGE_STATUS_START_TIME
  )
}

case class Opportunity(
    var id: Option[Long] = None,
    var pipelineId: Option[Long] = None,
    var phaseId: Option[Long] = None,
    var stageId: Option[Long] = None,
    var name: Option[String] = None,
    var customerId: Option[String] = None,
    var customerCid: Option[String] = None,
    var position: Option[Double] = None,
    var neighborhoodIds: Option[Seq[String]] = None,
    var areaIds: Option[Seq[String]] = None,
    var processTasksDone: Option[Seq[Long]] = None,
    @Deprecated
    var projectId: Option[String] = None,
    var propertyId: Option[String] = None,
    var estimatePrice: Option[Double] = None,
    var commissionType: Option[String] = None,
    var commissionValue: Option[Double] = None,
    var dueDate: Option[Long] = None,
    var closedDate: Option[Long] = None,
    var stageStartTime: Option[Long] = None,
    var note: Option[String] = None,
    var commissionPrice: Option[Double] = None,
    var pUserIds: Option[Seq[String]] = None,
    var pTeamIds: Option[Seq[String]] = None,
    var pMcIds: Option[Seq[String]] = None,
    var locations: Option[Seq[JsonNode]] = None,
    var closedStatus: Option[String] = None,
    var createdTime: Option[Long] = None,
    var updatedTime: Option[Long] = None,
    var status: Option[Int] = None,
    var createdBy: Option[String] = None,
    var updatedBy: Option[String] = None,
    var locationName: Option[String] = None,
    var owner: Option[String] = None,
    var mcId: Option[String] = None,
    var ownerTeam: Option[String] = None,
    var ownerMc: Option[String] = None,
    var tagIds: Option[Seq[Long]] = None,
    var userRelatedIds: Option[Seq[String]] = None,
    var serviceType: Option[String] = None,
    var contactSource: Option[String] = None,
    var additionalInfo: Option[JsonNode] = None,
    var ownerDistributedJobId: Option[String] = None,
    var projectIds: Option[Seq[String]] = None,
    var closedBy: Option[String] = None,
    var propertyInfo: Option[JsonNode] = None,
    var transactionType: Option[String] = None,
    var affiliateCid: Option[String] = None,
    var affiliatePcid: Option[String] = None,
    var bookingValue: Option[Double] = None,
    var depositValue: Option[Double] = None,
    var listingType: Option[String] = None,
    var inquiryId: Option[String] = None,
    var purposes: Option[Set[String]] = None,
    var customerName: Option[String] = None,
    var customerIdentities: Option[JsonNode] = None,
    var priority: Option[Int] = None,
    var stageStatus: Option[Int] = None,
    var pipelineVersion: Option[Int] = None,
    var stageStatusStartTime: Option[Long] = None,
    /*Custom field*/
    var customerShortName: Option[String] = None,
    var totalTask: Option[Int] = None,
    var totalTaskDone: Option[Int] = None,
    var canEdit: Option[Boolean] = None,
    var tags: Option[Seq[JsonNode]] = None,
    var permissions: Option[JsonNode] = None,
    var stageDuration: Option[Long] = None,
    var failReasonMsg: Option[String] = None,
    var stageActivities: Option[Seq[JsonNode]] = None,
    var pipelineName: Option[String] = None,
    var hasApplyUserPermission: Boolean = false,
    var stageRequiredFields: Option[Seq[String]] = None,
    var movableStageIds: Option[Seq[Long]] = None
) extends JdbcRecord {

  override def getPrimaryKeys(): Seq[String] = Opportunity.PRIMARY_IDS

  override def getFields(): Seq[String] = Opportunity.FIELDS

  override def setValues(field: String, value: Any): Unit = field match {
    case Opportunity.ID                 => id = value.asOpt
    case Opportunity.POSITION           => position = value.asOpt
    case Opportunity.STATUS             => status = value.asOpt
    case Opportunity.CREATED_TIME       => createdTime = value.asOpt
    case Opportunity.UPDATED_TIME       => updatedTime = value.asOpt
    case Opportunity.CREATED_BY         => createdBy = value.asOpt
    case Opportunity.UPDATED_BY         => updatedBy = value.asOpt
    case Opportunity.PIPELINE_ID        => pipelineId = value.asOpt
    case Opportunity.PIPELINE_VERSION   => pipelineVersion = value.asOpt
    case Opportunity.PHASE_ID           => phaseId = value.asOpt
    case Opportunity.STAGE_ID           => stageId = value.asOpt
    case Opportunity.NAME               => name = value.asOpt
    case Opportunity.CUSTOMER_ID        => customerId = value.asOpt
    case Opportunity.NEIGHBORHOOD_IDS   => neighborhoodIds = value.asOptString.map(JsonUtils.fromJson[Seq[String]])
    case Opportunity.AREA_IDS           => areaIds = value.asOptString.map(JsonUtils.fromJson[Seq[String]])
    case Opportunity.PROCESS_TASKS_DONE => processTasksDone = value.asOptString.map(JsonUtils.fromJson[Seq[Long]])
    case Opportunity.CUSTOMER_IDENTITIES =>
      customerIdentities = value.asOptString.map(JsonUtils.fromJson[JsonNode])
    case Opportunity.TRANSACTION_TYPE         => transactionType = value.asOpt
    case Opportunity.AFFILIATE_CID            => affiliateCid = value.asOpt
    case Opportunity.AFFILIATE_PCID           => affiliatePcid = value.asOpt
    case Opportunity.BOOKING_VALUE            => bookingValue = value.asOpt
    case Opportunity.DEPOSIT_VALUE            => depositValue = value.asOpt
    case Opportunity.STAGE_STATUS             => stageStatus = value.asOpt
    case Opportunity.LISTING_TYPE             => listingType = value.asOpt
    case Opportunity.INQUIRY_ID               => inquiryId = value.asOpt
    case Opportunity.PROJECT_ID               => projectId = value.asOpt
    case Opportunity.PROPERTY_ID              => propertyId = value.asOpt
    case Opportunity.ESTIMATE_PRICE           => estimatePrice = value.asOpt
    case Opportunity.COMMISSION_TYPE          => commissionType = value.asOpt
    case Opportunity.COMMISSION_VALUE         => commissionValue = value.asOpt
    case Opportunity.DUE_DATE                 => dueDate = value.asOpt
    case Opportunity.CLOSED_DATE              => closedDate = value.asOpt
    case Opportunity.STAGE_START_TIME         => stageStartTime = value.asOpt
    case Opportunity.NOTE                     => note = value.asOpt
    case Opportunity.CLOSED_STATUS            => closedStatus = value.asOpt
    case Opportunity.COMMISSION_PRICE         => commissionPrice = value.asOpt
    case Opportunity.PERMISSION_USER_IDS      => pUserIds = value.asOptString.map(JsonUtils.fromJson[Seq[String]])
    case Opportunity.PERMISSION_TEAM_IDS      => pTeamIds = value.asOptString.map(JsonUtils.fromJson[Seq[String]])
    case Opportunity.PERMISSION_MC_IDS        => pMcIds = value.asOptString.map(JsonUtils.fromJson[Seq[String]])
    case Opportunity.LOCATIONS                => locations = value.asOptString.map(JsonUtils.fromJson[Seq[JsonNode]])
    case Opportunity.TAG_IDS                  => tagIds = value.asOptString.map(JsonUtils.fromJson[Seq[Long]])
    case Opportunity.USER_RELATED_IDS         => userRelatedIds = value.asOptString.map(JsonUtils.fromJson[Seq[String]])
    case Opportunity.PURPOSES                 => purposes = value.asOptString.map(JsonUtils.fromJson[Set[String]])
    case Opportunity.PROJECT_IDS              => projectIds = value.asOptString.map(JsonUtils.fromJson[Seq[String]])
    case Opportunity.PROPERTY_INFO            => propertyInfo = value.asOptString.map(JsonUtils.fromJson[JsonNode])
    case Opportunity.LOCATION_NAME            => locationName = value.asOpt
    case Opportunity.OWNER                    => owner = value.asOpt
    case Opportunity.CLOSED_BY                => closedBy = value.asOpt
    case Opportunity.MC_ID                    => mcId = value.asOpt
    case Opportunity.OWNER_TEAM               => ownerTeam = value.asOpt
    case Opportunity.OWNER_MC                 => ownerMc = value.asOpt
    case Opportunity.CUSTOMER_CID             => customerCid = value.asOpt
    case Opportunity.SERVICE_TYPE             => serviceType = value.asOpt
    case Opportunity.CONTACT_SOURCE           => contactSource = value.asOpt
    case Opportunity.CUSTOMER_NAME            => customerName = value.asOpt
    case Opportunity.PRIORITY                 => priority = value.asOpt
    case Opportunity.OWNER_DISTRIBUTED_JOB_ID => ownerDistributedJobId = value.asOpt
    case Opportunity.STAGE_STATUS_START_TIME  => stageStatusStartTime = value.asOpt
    case Opportunity.ADDITIONAL_INFO          => additionalInfo = value.asOptString.map(JsonUtils.fromJson[JsonNode])
    case _                                    => throw SqlFieldMissing(field, value)
  }

  override def getValue(field: String): Option[Any] = field match {
    case Opportunity.ID                       => id
    case Opportunity.POSITION                 => position
    case Opportunity.STATUS                   => status
    case Opportunity.CREATED_TIME             => createdTime
    case Opportunity.UPDATED_TIME             => updatedTime
    case Opportunity.CREATED_BY               => createdBy
    case Opportunity.UPDATED_BY               => updatedBy
    case Opportunity.PIPELINE_ID              => pipelineId
    case Opportunity.PIPELINE_VERSION         => pipelineVersion
    case Opportunity.PHASE_ID                 => phaseId
    case Opportunity.STAGE_ID                 => stageId
    case Opportunity.NAME                     => name
    case Opportunity.CUSTOMER_ID              => customerId
    case Opportunity.NEIGHBORHOOD_IDS         => neighborhoodIds.map(JsonUtils.toJson(_))
    case Opportunity.AREA_IDS                 => areaIds.map(JsonUtils.toJson(_))
    case Opportunity.PROCESS_TASKS_DONE       => processTasksDone.map(JsonUtils.toJson(_))
    case Opportunity.CUSTOMER_IDENTITIES      => customerIdentities.map(JsonUtils.toJson(_))
    case Opportunity.TRANSACTION_TYPE         => transactionType
    case Opportunity.AFFILIATE_CID            => affiliateCid
    case Opportunity.AFFILIATE_PCID           => affiliatePcid
    case Opportunity.BOOKING_VALUE            => bookingValue
    case Opportunity.DEPOSIT_VALUE            => depositValue
    case Opportunity.STAGE_STATUS             => stageStatus
    case Opportunity.LISTING_TYPE             => listingType
    case Opportunity.INQUIRY_ID               => inquiryId
    case Opportunity.PROJECT_ID               => projectId
    case Opportunity.PROPERTY_ID              => propertyId
    case Opportunity.ESTIMATE_PRICE           => estimatePrice
    case Opportunity.COMMISSION_TYPE          => commissionType
    case Opportunity.COMMISSION_VALUE         => commissionValue
    case Opportunity.DUE_DATE                 => dueDate
    case Opportunity.CLOSED_DATE              => closedDate
    case Opportunity.STAGE_START_TIME         => stageStartTime
    case Opportunity.NOTE                     => note
    case Opportunity.COMMISSION_PRICE         => commissionPrice
    case Opportunity.PERMISSION_USER_IDS      => pUserIds.map(JsonUtils.toJson(_))
    case Opportunity.PERMISSION_TEAM_IDS      => pTeamIds.map(JsonUtils.toJson(_))
    case Opportunity.PERMISSION_MC_IDS        => pMcIds.map(JsonUtils.toJson(_))
    case Opportunity.LOCATIONS                => locations.map(JsonUtils.toJson(_))
    case Opportunity.TAG_IDS                  => tagIds.map(JsonUtils.toJson(_))
    case Opportunity.USER_RELATED_IDS         => userRelatedIds.map(JsonUtils.toJson(_))
    case Opportunity.PURPOSES                 => purposes.map(JsonUtils.toJson(_))
    case Opportunity.PROJECT_IDS              => projectIds.map(JsonUtils.toJson(_))
    case Opportunity.PROPERTY_INFO            => propertyInfo.map(JsonUtils.toJson(_))
    case Opportunity.CLOSED_STATUS            => closedStatus
    case Opportunity.LOCATION_NAME            => locationName
    case Opportunity.OWNER                    => owner
    case Opportunity.CLOSED_BY                => closedBy
    case Opportunity.MC_ID                    => mcId
    case Opportunity.OWNER_TEAM               => ownerTeam
    case Opportunity.OWNER_MC                 => ownerMc
    case Opportunity.CUSTOMER_CID             => customerCid
    case Opportunity.SERVICE_TYPE             => serviceType
    case Opportunity.CONTACT_SOURCE           => contactSource
    case Opportunity.CUSTOMER_NAME            => customerName
    case Opportunity.PRIORITY                 => priority
    case Opportunity.ADDITIONAL_INFO          => additionalInfo.map(JsonUtils.toJson(_))
    case Opportunity.OWNER_DISTRIBUTED_JOB_ID => ownerDistributedJobId
    case Opportunity.STAGE_STATUS_START_TIME  => stageStatusStartTime
    case _                                    => throw SqlFieldMissing(field)
  }

  def toMap: Map[String, Any] = {
    Opportunity.FIELDS
      .map(field => field -> getValue(field).get)
      .toMap
  }

}
