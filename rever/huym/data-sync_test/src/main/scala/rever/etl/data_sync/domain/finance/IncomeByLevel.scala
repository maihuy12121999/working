package rever.etl.data_sync.domain.finance

import rever.etl.data_sync.util.Utils.ImplicitAny
import vn.rever.jdbc.{JdbcRecord, SqlFieldMissing}

object IncomeByLevel {

  final val TBL_NAME = "income_by_level"

  def field(f: String): String = f

  final val PERIOD_TYPE: String = field("period_type")
  final val PERIOD_VALUE: String = field("period_value")
  final val BUSINESS_UNIT: String = field("business_unit")
  final val LEVEL: String = field("level")
  final val AMOUNT: String = field("amount")
  final val HEADCOUNT: String = field("headcount")
  final val TIME: String = field("time")
  final val LOG_TIME: String = field("log_time")

  final val primaryKeys = Seq(PERIOD_TYPE, PERIOD_VALUE)

  final val fields = Seq(
    PERIOD_TYPE,
    PERIOD_VALUE,
    BUSINESS_UNIT,
    LEVEL,
    AMOUNT,
    HEADCOUNT,
    TIME,
    LOG_TIME
  )

  def empty: IncomeByLevel = {
    IncomeByLevel(
      periodType = None,
      periodValue = None,
      businessUnit = None,
      level = None,
      amount = None,
      headCount = None,
      time = None,
      logTime = None
    )
  }
}

case class IncomeByLevel(
    var periodType: Option[String],
    var periodValue: Option[String],
    var businessUnit: Option[String],
    var level: Option[String],
    var amount: Option[Double],
    var headCount: Option[Int],
    var time: Option[Long],
    var logTime: Option[Long]
) extends JdbcRecord {

  override def getPrimaryKeys(): Seq[String] = IncomeByLevel.primaryKeys

  override def getFields(): Seq[String] = IncomeByLevel.fields

  override def setValues(field: String, value: Any): Unit =
    field match {
      case IncomeByLevel.PERIOD_TYPE   => periodType = value.asOpt
      case IncomeByLevel.PERIOD_VALUE  => periodValue = value.asOpt
      case IncomeByLevel.BUSINESS_UNIT => businessUnit = value.asOpt
      case IncomeByLevel.LEVEL         => level = value.asOpt
      case IncomeByLevel.AMOUNT        => amount = value.asOpt
      case IncomeByLevel.HEADCOUNT     => headCount = value.asOpt
      case IncomeByLevel.TIME          => time = value.asOpt
      case IncomeByLevel.LOG_TIME      => logTime = value.asOpt
      case _                           => throw SqlFieldMissing(field)
    }

  override def getValue(field: String): Option[Any] =
    field match {
      case IncomeByLevel.PERIOD_TYPE   => periodType
      case IncomeByLevel.PERIOD_VALUE  => periodValue
      case IncomeByLevel.BUSINESS_UNIT => businessUnit
      case IncomeByLevel.LEVEL         => level
      case IncomeByLevel.AMOUNT        => amount
      case IncomeByLevel.HEADCOUNT     => headCount
      case IncomeByLevel.TIME          => time
      case IncomeByLevel.LOG_TIME      => logTime
      case _                           => throw SqlFieldMissing(field)
    }
}
