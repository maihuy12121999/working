package rever.etl.data_sync.domain.hrm

import com.fasterxml.jackson.databind.JsonNode
import rever.etl.rsparkflow.utils.TimestampUtils

import scala.concurrent.duration.DurationInt

object TimeOffDetail {
  def fromJsonNode(node: JsonNode): TimeOffDetail = {
    val date = node.at("/date").asText("")
    val shift = node.at("/shift").asInt(0)

    val baseTime = TimestampUtils.parseMillsFromString(date, "dd-MM-yyyy")
    val (start, end) = shift match {
      case 0 => (baseTime, baseTime + 1.days.toMillis)
      case 1 => (baseTime, baseTime + 1.days.toMillis / 2)
      case 2 => (baseTime + 1.days.toMillis / 2, baseTime + 1.days.toMillis)
      case _ => throw new Exception(s"Unknown Time Off ShiftType: ${shift}")
    }
    val shiftName = toShiftName(shift)

    TimeOffDetail(date, baseTime, shift, shiftName, start, end)
  }

  private def toShiftName(shift: Int): String = {
    shift match {
      case 0 => "full_day"
      case 1 => "morning"
      case 2 => "afternoon"
      case _ => throw new Exception(s"Unknown Time Off ShiftType: ${shift}")
    }
  }
}

case class TimeOffDetail(
    date: String,
    timestamp: Long,
    shift: Int,
    shiftName: String,
    approxStartTime: Long,
    approxEndTime: Long
)
