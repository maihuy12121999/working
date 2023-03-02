package rever.etl.data_sync.domain.hrm

import com.fasterxml.jackson.databind.JsonNode
import rever.etl.data_sync.domain.GenericRecord
import vn.rever.common.domain.Implicits.ImplicitAny
import vn.rever.common.util.JsonHelper
import vn.rever.jdbc.{JdbcRecord, SqlFieldMissing}

import scala.jdk.CollectionConverters.asScalaIteratorConverter

object TimeOffDM {
  final val TBL_NAME = "booking"

  final val ID = "id"
  final val USERNAME = "username"
  final val TIME_OFF_TYPE = "time_off_type"
  final val DESCRIPTION = "description"
  final val MARKET_CENTER = "market_center_id"
  final val TEAM = "team_id"
  final val MANAGER = "manager"
  final val ACCRUALS = "accruals"
  final val BOOKING_DETAIL = "booking_detail"
  final val TIME_OFF_DETAIL = "time_off_detail"
  final val START_TIME = "start_time"
  final val END_TIME = "end_time"
  final val TOTAL_TIME_OFF = "total_time_off"
  final val BOOKING_STATUS = "booking_status"
  final val APPROVER = "approver"
  final val REJECT_REASON = "reject_reason"
  final val STATUS = "status"
  final val CREATED_TIME = "created_time"
  final val CREATED_BY = "created_by"
  final val UPDATED_TIME = "updated_time"
  final val UPDATED_BY = "updated_by"

  final val PRIMARY_IDS = Seq(ID)

  final val FIELDS = Seq(
    ID,
    USERNAME,
    TIME_OFF_TYPE,
    DESCRIPTION,
    MARKET_CENTER,
    TEAM,
    MANAGER,
    ACCRUALS,
    BOOKING_DETAIL,
    TIME_OFF_DETAIL,
    START_TIME,
    END_TIME,
    TOTAL_TIME_OFF,
    BOOKING_STATUS,
    APPROVER,
    REJECT_REASON,
    STATUS,
    CREATED_TIME,
    CREATED_BY,
    UPDATED_TIME,
    UPDATED_BY
  )

}

object TimeOff {
  final val TBL_NAME = "booking"

  final val ID = "id"
  final val USERNAME = "username"
  final val TYPE_ID = "time_off_type_id"
  final val DESCRIPTION = "desc"
  final val MARKET_CENTER = "market_center"
  final val TEAM = "team"
  final val MANAGER = "manager"
  final val ACCRUALS = "accruals"
  final val BOOKING_DETAIL = "booking_detail"
  final val START_TIME = "start_time"
  final val END_TIME = "end_time"
  final val TOTAL_TIME_OFF = "total_time_off"
  final val BOOKING_STATUS = "booking_status"
  final val APPROVER = "approver"
  final val REJECT_REASON = "reject_reason"
  final val STATUS = "status"
  final val CREATED_TIME = "created_time"
  final val CREATED_BY = "created_by"
  final val UPDATED_TIME = "updated_time"
  final val UPDATED_BY = "updated_by"

  final val PRIMARY_IDS = Seq(ID)

  final val FIELDS = Seq(
    ID,
    USERNAME,
    TYPE_ID,
    DESCRIPTION,
    MARKET_CENTER,
    TEAM,
    MANAGER,
    ACCRUALS,
    BOOKING_DETAIL,
    START_TIME,
    END_TIME,
    TOTAL_TIME_OFF,
    BOOKING_STATUS,
    APPROVER,
    REJECT_REASON,
    STATUS,
    CREATED_TIME,
    CREATED_BY,
    UPDATED_TIME,
    UPDATED_BY
  )

  final def fromGenericRecord(record: GenericRecord): TimeOff = {
    val contact = TimeOff()

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

case class TimeOff(
    var id: Option[String] = None,
    var username: Option[String] = None,
    var typeId: Option[Int] = None,
    var marketCenterId: Option[String] = None,
    var teamId: Option[String] = None,
    var manager: Option[String] = None,
    var accruals: Option[Seq[JsonNode]] = None,
    var bookingDetail: Option[JsonNode] = None,
    var description: Option[String] = None,
    var startTime: Option[Long] = None,
    var endTime: Option[Long] = None,
    var totalTimeOff: Option[Int] = None,
    var bookingStatus: Option[Int] = None,
    var approver: Option[String] = None,
    var rejectReason: Option[String] = None,
    var status: Option[Int] = None,
    var createdTime: Option[Long] = None,
    var createdBy: Option[String] = None,
    var updatedTime: Option[Long] = None,
    var updatedBy: Option[String] = None
) extends JdbcRecord {

  override def getPrimaryKeys(): Seq[String] = TimeOff.PRIMARY_IDS

  override def getFields(): Seq[String] = TimeOff.FIELDS

  override def setValues(field: String, value: Any): Unit = field match {
    case TimeOff.ID             => id = value.asOpt
    case TimeOff.USERNAME       => username = value.asOpt
    case TimeOff.TYPE_ID        => typeId = value.asOpt
    case TimeOff.MARKET_CENTER  => marketCenterId = value.asOpt
    case TimeOff.TEAM           => teamId = value.asOpt
    case TimeOff.MANAGER        => manager = value.asOpt
    case TimeOff.ACCRUALS       => accruals = value.asOptString.map(JsonHelper.fromJson[Seq[JsonNode]](_))
    case TimeOff.BOOKING_DETAIL => bookingDetail = value.asOptString.map(JsonHelper.fromJson[JsonNode])
    case TimeOff.DESCRIPTION    => description = value.asOpt
    case TimeOff.START_TIME     => startTime = value.asOpt
    case TimeOff.END_TIME       => endTime = value.asOpt
    case TimeOff.TOTAL_TIME_OFF => totalTimeOff = value.asOpt
    case TimeOff.BOOKING_STATUS => bookingStatus = value.asOpt
    case TimeOff.APPROVER       => approver = value.asOpt
    case TimeOff.REJECT_REASON  => rejectReason = value.asOpt
    case TimeOff.STATUS         => status = value.asOpt
    case TimeOff.CREATED_TIME   => createdTime = value.asOpt
    case TimeOff.CREATED_BY     => createdBy = value.asOpt
    case TimeOff.UPDATED_TIME   => updatedTime = value.asOpt
    case TimeOff.UPDATED_BY     => updatedBy = value.asOpt
    case _                      =>
  }

  override def getValue(field: String): Option[Any] = field match {
    case TimeOff.ID             => id
    case TimeOff.USERNAME       => username
    case TimeOff.TYPE_ID        => typeId
    case TimeOff.MARKET_CENTER  => marketCenterId
    case TimeOff.TEAM           => teamId
    case TimeOff.MANAGER        => manager
    case TimeOff.ACCRUALS       => accruals.map(JsonHelper.toJson(_, pretty = false))
    case TimeOff.BOOKING_DETAIL => bookingDetail.map(_.toString)
    case TimeOff.DESCRIPTION    => description
    case TimeOff.START_TIME     => startTime
    case TimeOff.END_TIME       => endTime
    case TimeOff.TOTAL_TIME_OFF => totalTimeOff
    case TimeOff.BOOKING_STATUS => bookingStatus
    case TimeOff.APPROVER       => approver
    case TimeOff.REJECT_REASON  => rejectReason
    case TimeOff.STATUS         => status
    case TimeOff.CREATED_TIME   => createdTime
    case TimeOff.CREATED_BY     => createdBy
    case TimeOff.UPDATED_TIME   => updatedTime
    case TimeOff.UPDATED_BY     => updatedBy
    case _                      => throw SqlFieldMissing(field)
  }

  def getTimeOffDetail: Seq[TimeOffDetail] = {
    bookingDetail
      .map(_.at("/detail_time_off").elements().asScala.toSeq)
      .getOrElse(Seq.empty)
      .filterNot(_.isNull)
      .filterNot(_.isMissingNode)
      .filterNot(_.isEmpty)
      .map(TimeOffDetail.fromJsonNode)
      .toSeq
  }

  def toMap: Map[String, Any] = {
    TimeOff.FIELDS
      .map(field => field -> getValue(field).get)
      .toMap
  }

}
