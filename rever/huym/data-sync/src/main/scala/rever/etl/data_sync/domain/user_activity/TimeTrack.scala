package rever.etl.data_sync.domain.user_activity

import com.fasterxml.jackson.databind.JsonNode
import rever.etl.data_sync.domain.GenericRecord
import vn.rever.common.domain.Implicits.ImplicitAny
import vn.rever.common.util.JsonHelper
import vn.rever.jdbc.{JdbcRecord, SqlFieldMissing}

object TimeTrackDM {
  final val ID = "id"
  final val USERNAME = "username"
  final val WORK_EMAIL = "email"
  final val TIME = "time"
  final val TIME_STRING = "time_string"
  final val SCHEDULER_ID = "scheduler_id"
  final val MARKET_CENTER_ID = "market_center_id"
  final val TEAM_ID = "team_id"
  final val WORK_DURATION = "work_duration"
  final val FIRST_IN_TIME = "first_in_time"
  final val LAST_OUT_TIME = "last_out_time"
  final val LATEST_CLOCK_TIME = "latest_clock_time"
  final val CLOCKS = "clocks"
  final val CLOCK_IN = "clock_in"
  final val CLOCK_OUT = "clock_out"
  final val STATUS = "status"

  final val CREATED_TIME = "created_time"
  final val CREATED_BY = "created_by"
  final val UPDATED_TIME = "updated_time"
  final val UPDATED_BY = "updated_by"
  final val LOG_TIME = "log_time"

  final val PRIMARY_IDS = Seq(ID)

  final val FIELDS = Seq(
    ID,
    USERNAME,
    WORK_EMAIL,
    TIME,
    TIME_STRING,
    SCHEDULER_ID,
    MARKET_CENTER_ID,
    TEAM_ID,
    WORK_DURATION,
    FIRST_IN_TIME,
    LAST_OUT_TIME,
    LATEST_CLOCK_TIME,
    CLOCKS,
    STATUS,
    CREATED_TIME,
    CREATED_BY,
    UPDATED_TIME,
    UPDATED_BY,
    LOG_TIME
  )
}

object TimeTrack {
  final val TBL_NAME = "time_track"

  final val ID = "id"
  final val USERNAME = "username"
  final val WORK_EMAIL = "email"
  final val TIME = "time"
  final val TIME_STRING = "time_string"
  final val SCHEDULER_ID = "scheduler_id"
  final val MARKET_CENTER_ID = "market_center_id"
  final val TEAM_ID = "team"
  final val WORK_DURATION = "work_duration"
  final val FIRST_IN_TIME = "first_in_time"
  final val LAST_OUT_TIME = "last_out_time"
  final val LATEST_CLOCK_TIME = "latest_clock_time"
  final val CLOCKS = "clocks"
  final val STATUS = "status"

  final val CREATED_TIME = "created_time"
  final val CREATED_BY = "created_by"
  final val UPDATED_TIME = "updated_time"
  final val UPDATED_BY = "updated_by"

  final val PRIMARY_IDS = Seq(ID)

  final val FIELDS = Seq(
    ID,
    USERNAME,
    WORK_EMAIL,
    TIME,
    TIME_STRING,
    SCHEDULER_ID,
    MARKET_CENTER_ID,
    TEAM_ID,
    WORK_DURATION,
    FIRST_IN_TIME,
    LAST_OUT_TIME,
    LATEST_CLOCK_TIME,
    CLOCKS,
    STATUS,
    CREATED_TIME,
    CREATED_BY,
    UPDATED_TIME,
    UPDATED_BY
  )

  final def fromGenericRecord(record: GenericRecord): TimeTrack = {
    val contact = TimeTrack()

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

case class TimeTrack(
    var id: Option[String] = None,
    var username: Option[String] = None,
    var workEmail: Option[String] = None,
    var time: Option[Long] = None,
    var timeString: Option[String] = None,
    var schedulerId: Option[Int] = None,
    var marketCenterId: Option[String] = None,
    var teamId: Option[String] = None,
    var workDuration: Option[String] = None,
    var firstInTime: Option[Long] = None,
    var lastOutTime: Option[Long] = None,
    var clocks: Option[Seq[JsonNode]] = None,
    var latestClockTime: Option[Long] = None,
    var status: Option[Int] = None,
    var createdTime: Option[Long] = None,
    var createdBy: Option[String] = None,
    var updatedTime: Option[Long] = None,
    var updatedBy: Option[String] = None
) extends JdbcRecord {

  override def getPrimaryKeys(): Seq[String] = TimeTrack.PRIMARY_IDS

  override def getFields(): Seq[String] = TimeTrack.FIELDS

  override def setValues(field: String, value: Any): Unit = field match {
    case TimeTrack.ID                => id = value.asOpt
    case TimeTrack.USERNAME          => username = value.asOpt
    case TimeTrack.WORK_EMAIL        => workEmail = value.asOpt
    case TimeTrack.TIME              => time = value.asOpt
    case TimeTrack.TIME_STRING       => timeString = value.asOpt
    case TimeTrack.SCHEDULER_ID      => schedulerId = value.asOpt
    case TimeTrack.MARKET_CENTER_ID  => marketCenterId = value.asOpt
    case TimeTrack.TEAM_ID           => teamId = value.asOpt
    case TimeTrack.WORK_DURATION     => workDuration = value.asOpt
    case TimeTrack.FIRST_IN_TIME     => firstInTime = value.asOpt
    case TimeTrack.LAST_OUT_TIME     => lastOutTime = value.asOpt
    case TimeTrack.LATEST_CLOCK_TIME => latestClockTime = value.asOpt
    case TimeTrack.CLOCKS            => clocks = value.asOptString.map(JsonHelper.fromJson[Seq[JsonNode]])
    case TimeTrack.STATUS            => status = value.asOpt
    case TimeTrack.CREATED_TIME      => createdTime = value.asOpt
    case TimeTrack.CREATED_BY        => createdBy = value.asOpt
    case TimeTrack.UPDATED_TIME      => updatedTime = value.asOpt
    case TimeTrack.UPDATED_BY        => updatedBy = value.asOpt
    case _                           =>
  }

  override def getValue(field: String): Option[Any] = field match {
    case TimeTrack.ID                => id
    case TimeTrack.USERNAME          => username
    case TimeTrack.WORK_EMAIL        => workEmail
    case TimeTrack.TIME              => time
    case TimeTrack.TIME_STRING       => timeString
    case TimeTrack.SCHEDULER_ID      => schedulerId
    case TimeTrack.MARKET_CENTER_ID  => marketCenterId
    case TimeTrack.TEAM_ID           => teamId
    case TimeTrack.WORK_DURATION     => workDuration
    case TimeTrack.FIRST_IN_TIME     => firstInTime
    case TimeTrack.LAST_OUT_TIME     => lastOutTime
    case TimeTrack.LATEST_CLOCK_TIME => latestClockTime
    case TimeTrack.CLOCKS            => clocks.map(JsonHelper.toJson(_, pretty = false))
    case TimeTrack.STATUS            => status
    case TimeTrack.CREATED_TIME      => createdTime
    case TimeTrack.CREATED_BY        => createdBy
    case TimeTrack.UPDATED_TIME      => updatedTime
    case TimeTrack.UPDATED_BY        => updatedBy
    case _                           => throw SqlFieldMissing(field)
  }

  def toMap: Map[String, Any] = {
    TimeTrack.FIELDS
      .map(field => field -> getValue(field).get)
      .toMap
  }

  def getTimeTrackStatusName(): String = {
    status match {
      case Some(x) if x == 0 => "create"
      case Some(x) if x == 1 => "clock_in"
      case Some(x) if x == 2 => "clock_out"
      case _                 => throw new Exception(s"Unknow TimeTrack status: ${status}")
    }
  }

  def getClockIn: Option[JsonNode] = {
    clocks.getOrElse(Seq.empty) lift 0
  }

  def getClockOut: Option[JsonNode] = {
    clocks.getOrElse(Seq.empty) lift 1
  }

}
