package rever.etl.data_sync.domain.user_activity

import rever.etl.data_sync.domain.GenericRecord
import vn.rever.common.domain.Implicits.ImplicitAny
import vn.rever.jdbc.{JdbcRecord, SqlFieldMissing}

object ClockActionType extends Enumeration {
  type ClockActionType = Value

  final val TYPE_CLOCK_IN = Value(1, "clock in")
  final val TYPE_CLOCK_OUT = Value(2, "clock out")

  def opt(id: Int): Option[ClockActionType] = values.find(p => p.id == id)

  def clockActionName(clockType: Int): String = {
    opt(clockType) match {
      case Some(TYPE_CLOCK_IN)  => "clock_in"
      case Some(TYPE_CLOCK_OUT) => "clock_out"
      case _                    => "undefined"
    }
  }
}

object ClockInOutRequest {
  final val TBL_NAME = "clock_request"

  final val ID = "id"
  final val TYPE = "type"
  final val SOURCE = "source"
  final val SOURCE_USER = "source_user"
  final val SOURCE_CHANNEL = "source_channel"
  final val USERNAME = "username"
  final val WORK_EMAIL = "email"
  final val TIME = "time"
  final val VERIFIED_TIME = "verified_time"
  final val STATUS = "status"

  final val PRIMARY_IDS = Seq(ID)

  final val FIELDS = Seq(
    ID,
    TYPE,
    SOURCE,
    SOURCE_USER,
    SOURCE_CHANNEL,
    USERNAME,
    WORK_EMAIL,
    TIME,
    VERIFIED_TIME,
    STATUS
  )

  final def fromGenericRecord(record: GenericRecord): ClockInOutRequest = {
    val contact = ClockInOutRequest()

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

case class ClockInOutRequest(
    var id: Option[String] = None,
    var clockType: Option[Int] = None,
    var source: Option[String] = None,
    var sourceUser: Option[String] = None,
    var sourceChannel: Option[String] = None,
    var username: Option[String] = None,
    var workEmail: Option[String] = None,
    var time: Option[Long] = None,
    var verifiedTime: Option[Long] = None,
    var status: Option[Int] = None
) extends JdbcRecord {

  override def getPrimaryKeys(): Seq[String] = ClockInOutRequest.PRIMARY_IDS

  override def getFields(): Seq[String] = ClockInOutRequest.FIELDS

  override def setValues(field: String, value: Any): Unit = field match {
    case ClockInOutRequest.ID             => id = value.asOpt
    case ClockInOutRequest.TYPE           => clockType = value.asOpt
    case ClockInOutRequest.SOURCE         => source = value.asOpt
    case ClockInOutRequest.SOURCE_USER    => sourceUser = value.asOpt
    case ClockInOutRequest.SOURCE_CHANNEL => sourceChannel = value.asOpt
    case ClockInOutRequest.USERNAME       => username = value.asOpt
    case ClockInOutRequest.WORK_EMAIL     => workEmail = value.asOpt
    case ClockInOutRequest.TIME           => time = value.asOpt
    case ClockInOutRequest.VERIFIED_TIME  => verifiedTime = value.asOpt
    case ClockInOutRequest.STATUS         => status = value.asOpt
    case _                                =>
  }

  override def getValue(field: String): Option[Any] = field match {
    case ClockInOutRequest.ID             => id
    case ClockInOutRequest.TYPE           => clockType
    case ClockInOutRequest.SOURCE         => source
    case ClockInOutRequest.SOURCE_USER    => sourceUser
    case ClockInOutRequest.SOURCE_CHANNEL => sourceChannel
    case ClockInOutRequest.USERNAME       => username
    case ClockInOutRequest.WORK_EMAIL     => workEmail
    case ClockInOutRequest.TIME           => time
    case ClockInOutRequest.VERIFIED_TIME  => verifiedTime
    case ClockInOutRequest.STATUS         => status
    case _                                => throw SqlFieldMissing(field)
  }

  def toMap: Map[String, Any] = {
    ClockInOutRequest.FIELDS
      .map(field => field -> getValue(field).get)
      .toMap
  }

}
