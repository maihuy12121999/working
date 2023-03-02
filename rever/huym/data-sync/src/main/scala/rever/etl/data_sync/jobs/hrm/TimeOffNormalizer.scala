package rever.etl.data_sync.jobs.hrm

import rever.etl.data_sync.core.Normalizer
import rever.etl.data_sync.domain.GenericRecord
import rever.etl.data_sync.domain.hrm.{TimeOff, TimeOffDM}
import vn.rever.common.util.JsonHelper

/** @author anhlt (andy)
  */
case class TimeOffNormalizer() extends Normalizer[GenericRecord, Map[String, Any]] {

  override def toRecord(record: GenericRecord, total: Long): Option[Map[String, Any]] = {

    val timeOff = TimeOff.fromGenericRecord(record)

    val dataMap = Map[String, Any](
      TimeOffDM.ID -> timeOff.id.get,
      TimeOffDM.TIME_OFF_TYPE -> timeOff.typeId.get,
      TimeOffDM.USERNAME -> timeOff.username.getOrElse(""),
      TimeOffDM.MARKET_CENTER -> timeOff.marketCenterId.getOrElse(""),
      TimeOffDM.TEAM -> timeOff.teamId.getOrElse(""),
      TimeOffDM.MANAGER -> timeOff.manager.getOrElse(""),
      TimeOffDM.ACCRUALS -> timeOff.accruals.map(_.map(_.toString)).getOrElse(Seq.empty).toArray,
      TimeOffDM.BOOKING_DETAIL -> timeOff.bookingDetail.map(_.toString).getOrElse("{}"),
      TimeOffDM.TIME_OFF_DETAIL -> timeOff.getTimeOffDetail.map(JsonHelper.toJson(_, pretty = false)).toArray,
      TimeOffDM.DESCRIPTION -> timeOff.description.getOrElse("{}"),
      TimeOffDM.START_TIME -> timeOff.startTime.getOrElse(0L),
      TimeOffDM.END_TIME -> timeOff.endTime.getOrElse(0L),
      TimeOffDM.TOTAL_TIME_OFF -> timeOff.totalTimeOff.getOrElse(0),
      TimeOffDM.BOOKING_STATUS -> timeOff.bookingStatus.map(toBookingStatusName).getOrElse(""),
      TimeOffDM.APPROVER -> timeOff.approver.getOrElse(""),
      TimeOffDM.REJECT_REASON -> timeOff.rejectReason.getOrElse(""),
      TimeOffDM.STATUS -> timeOff.status.getOrElse(0),
      TimeOffDM.CREATED_TIME -> timeOff.createdTime.getOrElse(0L),
      TimeOffDM.CREATED_BY -> timeOff.createdBy.getOrElse(""),
      TimeOffDM.UPDATED_TIME -> timeOff.updatedTime.getOrElse(0L),
      TimeOffDM.UPDATED_BY -> timeOff.updatedBy.getOrElse("")
    )
    Some(dataMap)
  }

  private def toBookingStatusName(bookingStatus: Int): String = {
    bookingStatus match {
      case 0 => "requesting"
      case 1 => "approved"
      case 2 => "rejected"
      case 3 => "cancelled"
      case _ => throw new Exception(s"Unknown Booking Status: ${bookingStatus}")
    }
  }

}
