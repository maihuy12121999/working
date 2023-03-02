package rever.etl.data_sync.domain.finance

import rever.etl.data_sync.util.Utils.ImplicitAny
import vn.rever.jdbc.{JdbcRecord, SqlFieldMissing}

object BookingRecord {

  final val TBL_NAME = "booking_report"

  def field(f: String): String = f

  final val BUSINESS_UNIT: String = field("business_unit")
  final val PERIOD_TYPE: String = field("period_type")
  final val PERIOD_VALUE: String = field("period_value")
  final val NUM_OF_TRANSACTIONS: String = field("num_of_transactions")
  final val REVENUE: String = field("revenue")
  final val PROFIT_AND_LOSS: String = field("profit_and_loss")
  final val TIME: String = field("time")
  final val LOG_TIME: String = field("log_time")

  final val primaryKeys = Seq(BUSINESS_UNIT, PERIOD_TYPE, PERIOD_VALUE)

  final val fields = Seq(
    BUSINESS_UNIT,
    PERIOD_TYPE,
    PERIOD_VALUE,
    NUM_OF_TRANSACTIONS,
    REVENUE,
    PROFIT_AND_LOSS,
    TIME,
    LOG_TIME
  )

  def empty: BookingRecord = {
    BookingRecord(
      businessUnit = None,
      periodType = None,
      periodValue = None,
      numOfTransactions = None,
      revenue = None,
      profitAndLoss = None,
      time = None,
      logTime = None
    )
  }
}

case class BookingRecord(
    var businessUnit: Option[String],
    var periodType: Option[String],
    var periodValue: Option[String],
    var numOfTransactions: Option[Int],
    var revenue: Option[Double],
    var profitAndLoss: Option[Double],
    var time: Option[Long],
    var logTime: Option[Long]
) extends JdbcRecord {

  override def getPrimaryKeys(): Seq[String] = BookingRecord.primaryKeys

  override def getFields(): Seq[String] = BookingRecord.fields

  override def setValues(field: String, value: Any): Unit =
    field match {
      case BookingRecord.BUSINESS_UNIT       => businessUnit = value.asOpt
      case BookingRecord.PERIOD_TYPE         => periodType = value.asOpt
      case BookingRecord.PERIOD_VALUE        => periodValue = value.asOpt
      case BookingRecord.NUM_OF_TRANSACTIONS => numOfTransactions = value.asOpt
      case BookingRecord.REVENUE             => revenue = value.asOpt
      case BookingRecord.PROFIT_AND_LOSS     => profitAndLoss = value.asOpt
      case BookingRecord.TIME                => time = value.asOpt
      case BookingRecord.LOG_TIME            => logTime = value.asOpt
      case _                                 => throw SqlFieldMissing(field)
    }

  override def getValue(field: String): Option[Any] = {
    field match {
      case BookingRecord.BUSINESS_UNIT       => businessUnit
      case BookingRecord.PERIOD_TYPE         => periodType
      case BookingRecord.PERIOD_VALUE        => periodValue
      case BookingRecord.NUM_OF_TRANSACTIONS => numOfTransactions
      case BookingRecord.REVENUE             => revenue
      case BookingRecord.PROFIT_AND_LOSS     => profitAndLoss
      case BookingRecord.TIME                => time
      case BookingRecord.LOG_TIME            => logTime
      case _                                 => throw SqlFieldMissing(field)
    }
  }
}
