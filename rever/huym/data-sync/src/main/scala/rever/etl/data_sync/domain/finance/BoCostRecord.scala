package rever.etl.data_sync.domain.finance

import rever.etl.data_sync.util.Utils.ImplicitAny
import vn.rever.jdbc.{JdbcRecord, SqlFieldMissing}

object BoCostRecord {

  final val TBL_NAME = "back_office_cost"

  def field(f: String): String = f

  final val KIND: String = field("data_kind")
  final val PERIOD_TYPE: String = field("period_type")
  final val PERIOD_VALUE: String = field("period_value")
  final val CATEGORY: String = field("category")
  final val COST_TYPE: String = field("cost_type")
  final val AMOUNT: String = field("amount")
  final val TIME: String = field("time")
  final val LOG_TIME: String = field("log_time")

  final val primaryKeys = Seq(KIND, PERIOD_TYPE, PERIOD_VALUE)

  final val fields = Seq(
    KIND,
    PERIOD_TYPE,
    PERIOD_VALUE,
    CATEGORY,
    COST_TYPE,
    AMOUNT,
    TIME,
    LOG_TIME
  )

  def empty: BoCostRecord = {
    BoCostRecord(
      kind = None,
      periodType = None,
      periodValue = None,
      category = None,
      costType = None,
      amount = None,
      time = None,
      logTime = None
    )
  }
}

case class BoCostRecord(
    var kind: Option[String],
    var periodType: Option[String],
    var periodValue: Option[String],
    var category: Option[String],
    var costType: Option[String],
    var amount: Option[Double],
    var time: Option[Long],
    var logTime: Option[Long]
) extends JdbcRecord {

  override def getPrimaryKeys(): Seq[String] = BoCostRecord.primaryKeys

  override def getFields(): Seq[String] = BoCostRecord.fields

  override def setValues(field: String, value: Any): Unit =
    field match {
      case BoCostRecord.KIND         => kind = value.asOpt
      case BoCostRecord.PERIOD_TYPE  => periodType = value.asOpt
      case BoCostRecord.PERIOD_VALUE => periodValue = value.asOpt
      case BoCostRecord.CATEGORY     => category = value.asOpt
      case BoCostRecord.COST_TYPE    => costType = value.asOpt
      case BoCostRecord.AMOUNT       => amount = value.asOpt
      case BoCostRecord.TIME         => time = value.asOpt
      case BoCostRecord.LOG_TIME     => logTime = value.asOpt
      case _                         => throw SqlFieldMissing(field)
    }

  override def getValue(field: String): Option[Any] =
    field match {
      case BoCostRecord.KIND         => kind
      case BoCostRecord.PERIOD_TYPE  => periodType
      case BoCostRecord.PERIOD_VALUE => periodValue
      case BoCostRecord.CATEGORY     => category
      case BoCostRecord.COST_TYPE    => costType
      case BoCostRecord.AMOUNT       => amount
      case BoCostRecord.TIME         => time
      case BoCostRecord.LOG_TIME     => logTime
      case _                         => throw SqlFieldMissing(field)
    }
}
