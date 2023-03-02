package rever.etl.data_sync.domain.user_activity

import rever.etl.data_sync.domain.GenericRecord
import vn.rever.common.domain.Implicits.ImplicitAny
import vn.rever.jdbc.{JdbcRecord, SqlFieldMissing}

object DailyCheckinDM {
  final val TBL_NAME = "daily_checkin"

  final val DATE_STRING = "date_string"
  final val USERNAME = "username"
  final val TIMESTAMP = "timestamp"
  final val MARKET_CENTER = "market_center_id"
  final val TEAM = "team_id"
  final val CHECKIN_STATUS = "checkin_status"
  final val FIRST_ANSWER = "first_answer"
  final val FIRST_ANSWER_AT = "first_answer_at"
  final val SECOND_ANSWER = "second_answer"
  final val SECOND_ANSWER_AT = "second_answer_at"
  final val THIRD_ANSWER = "third_answer"
  final val THIRD_ANSWER_AT = "third_answer_at"
  final val CREATED_TIME = "created_time"
  final val UPDATED_TIME = "updated_time"

  final val PRIMARY_IDS = Seq(DATE_STRING, USERNAME)

  final val FIELDS = Seq(
    DATE_STRING,
    TIMESTAMP,
    USERNAME,
    MARKET_CENTER,
    TEAM,
    FIRST_ANSWER,
    FIRST_ANSWER_AT,
    SECOND_ANSWER,
    SECOND_ANSWER_AT,
    THIRD_ANSWER,
    THIRD_ANSWER_AT,
    CHECKIN_STATUS,
    CREATED_TIME,
    UPDATED_TIME
  )

  def toCheckinStatusName(checkinStatus: Int): String = {
    checkinStatus match {
      case 0 => "draft"
      case 1 => "answering"
      case 2 => "done"
      case 3 => "cancelled"
      case _ => throw new Exception(s"Unknown Checkin Status: ${checkinStatus}")
    }
  }

}

object DailyCheckin {
  final val TBL_NAME = "daily_checkin"

  final val DATE_STRING = "date_string"
  final val USERNAME = "username"
  final val TIMESTAMP = "timestamp"
  final val MARKET_CENTER = "market_center"
  final val TEAM = "team"
  final val CHECKIN_STATUS = "checkin_status"
  final val FIRST_ANSWER = "first_answer"
  final val FIRST_ANSWER_AT = "first_answer_at"
  final val SECOND_ANSWER = "second_answer"
  final val SECOND_ANSWER_AT = "second_answer_at"
  final val THIRD_ANSWER = "third_answer"
  final val THIRD_ANSWER_AT = "third_answer_at"
  final val CREATED_TIME = "created_time"
  final val UPDATED_TIME = "updated_time"

  final val PRIMARY_IDS = Seq(DATE_STRING, USERNAME)

  final val FIELDS = Seq(
    DATE_STRING,
    TIMESTAMP,
    USERNAME,
    MARKET_CENTER,
    TEAM,
    FIRST_ANSWER,
    FIRST_ANSWER_AT,
    SECOND_ANSWER,
    SECOND_ANSWER_AT,
    THIRD_ANSWER,
    THIRD_ANSWER_AT,
    CHECKIN_STATUS,
    CREATED_TIME,
    UPDATED_TIME
  )

  final def fromGenericRecord(record: GenericRecord): DailyCheckin = {
    val contact = DailyCheckin()

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

case class DailyCheckin(
    var dateString: Option[String] = None,
    var timestamp: Option[Long] = None,
    var username: Option[String] = None,
    var marketCenterId: Option[String] = None,
    var teamId: Option[String] = None,
    var firstAnswer: Option[String] = None,
    var firstAnswerAt: Option[Long] = None,
    var secondAnswer: Option[String] = None,
    var secondAnswerAt: Option[Long] = None,
    var thirdAnswer: Option[String] = None,
    var thirdAnswerAt: Option[Long] = None,
    var checkinStatus: Option[Int] = None,
    var createdTime: Option[Long] = None,
    var updatedTime: Option[Long] = None
) extends JdbcRecord {

  override def getPrimaryKeys(): Seq[String] = DailyCheckin.PRIMARY_IDS

  override def getFields(): Seq[String] = DailyCheckin.FIELDS

  override def setValues(field: String, value: Any): Unit = field match {
    case DailyCheckin.DATE_STRING      => dateString = value.asOpt
    case DailyCheckin.TIMESTAMP        => timestamp = value.asOpt
    case DailyCheckin.USERNAME         => username = value.asOpt
    case DailyCheckin.MARKET_CENTER    => marketCenterId = value.asOpt
    case DailyCheckin.TEAM             => teamId = value.asOpt
    case DailyCheckin.FIRST_ANSWER     => firstAnswer = value.asOpt
    case DailyCheckin.FIRST_ANSWER_AT  => firstAnswerAt = value.asOpt
    case DailyCheckin.SECOND_ANSWER    => secondAnswer = value.asOpt
    case DailyCheckin.SECOND_ANSWER_AT => secondAnswerAt = value.asOpt
    case DailyCheckin.THIRD_ANSWER     => thirdAnswer = value.asOpt
    case DailyCheckin.THIRD_ANSWER_AT  => thirdAnswerAt = value.asOpt
    case DailyCheckin.CHECKIN_STATUS   => checkinStatus = value.asOpt
    case DailyCheckin.CREATED_TIME     => createdTime = value.asOpt
    case DailyCheckin.UPDATED_TIME     => updatedTime = value.asOpt
    case _                             =>
  }

  override def getValue(field: String): Option[Any] = field match {
    case DailyCheckin.DATE_STRING      => dateString
    case DailyCheckin.TIMESTAMP        => timestamp
    case DailyCheckin.USERNAME         => username
    case DailyCheckin.MARKET_CENTER    => marketCenterId
    case DailyCheckin.TEAM             => teamId
    case DailyCheckin.FIRST_ANSWER     => firstAnswer
    case DailyCheckin.FIRST_ANSWER_AT  => firstAnswerAt
    case DailyCheckin.SECOND_ANSWER    => secondAnswer
    case DailyCheckin.SECOND_ANSWER_AT => secondAnswerAt
    case DailyCheckin.THIRD_ANSWER     => thirdAnswer
    case DailyCheckin.THIRD_ANSWER_AT  => thirdAnswerAt
    case DailyCheckin.CHECKIN_STATUS   => checkinStatus
    case DailyCheckin.CREATED_TIME     => createdTime
    case DailyCheckin.UPDATED_TIME     => updatedTime
    case _                             => throw SqlFieldMissing(field)
  }

  def toMap: Map[String, Any] = {
    DailyCheckin.FIELDS
      .map(field => field -> getValue(field).get)
      .toMap
  }

}
