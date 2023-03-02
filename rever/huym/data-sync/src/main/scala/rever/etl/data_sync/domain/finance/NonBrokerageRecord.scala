package rever.etl.data_sync.domain.finance

import rever.etl.data_sync.util.Utils.ImplicitAny
import vn.rever.jdbc.{JdbcRecord, SqlFieldMissing}

object NonBrokerageRecord {

  final val TBL_NAME = "non_brokerage_report"

  def field(f: String): String = f
  final val KIND: String = field("data_kind")
  final val PERIOD_TYPE: String = field("period_type")
  final val PERIOD_VALUE: String = field("period_value")
  final val REVENUE: String = field("revenue")
  final val EXPENSE: String = field("expense")
  final val PROFIT_AND_LOSS: String = field("operating_profit_and_loss")
  final val TIME: String = field("time")
  final val LOG_TIME: String = field("log_time")

  final val primaryKeys = Seq(KIND, PERIOD_TYPE, PERIOD_VALUE)

  final val fields = Seq(
    KIND,
    PERIOD_TYPE,
    PERIOD_VALUE,
    REVENUE,
    EXPENSE,
    PROFIT_AND_LOSS,
    TIME,
    LOG_TIME
  )

  def empty: NonBrokerageRecord = {
    NonBrokerageRecord(
      kind = None,
      periodType = None,
      periodValue = None,
      revenue = None,
      expense = None,
      operatingProfitAndLoss = None,
      time = None,
      logTime = None
    )
  }
}

case class NonBrokerageRecord(
    var kind: Option[String],
    var periodType: Option[String],
    var periodValue: Option[String],
    var revenue: Option[Double],
    var expense: Option[Double],
    var operatingProfitAndLoss: Option[Double],
    var time: Option[Long],
    var logTime: Option[Long]
) extends JdbcRecord {

  override def getPrimaryKeys(): Seq[String] = NonBrokerageRecord.primaryKeys

  override def getFields(): Seq[String] = NonBrokerageRecord.fields

  override def setValues(field: String, value: Any): Unit =
    field match {
      case NonBrokerageRecord.KIND            => kind = value.asOpt
      case NonBrokerageRecord.PERIOD_TYPE     => periodType = value.asOpt
      case NonBrokerageRecord.PERIOD_VALUE    => periodValue = value.asOpt
      case NonBrokerageRecord.REVENUE         => revenue = value.asOpt
      case NonBrokerageRecord.EXPENSE         => expense = value.asOpt
      case NonBrokerageRecord.PROFIT_AND_LOSS => operatingProfitAndLoss = value.asOpt
      case NonBrokerageRecord.TIME            => time = value.asOpt
      case NonBrokerageRecord.LOG_TIME        => logTime = value.asOpt
      case _                                  => throw SqlFieldMissing(field)
    }

  override def getValue(field: String): Option[Any] =
    field match {
      case NonBrokerageRecord.KIND            => kind
      case NonBrokerageRecord.PERIOD_TYPE     => periodType
      case NonBrokerageRecord.PERIOD_VALUE    => periodValue
      case NonBrokerageRecord.REVENUE         => revenue
      case NonBrokerageRecord.EXPENSE         => expense
      case NonBrokerageRecord.PROFIT_AND_LOSS => operatingProfitAndLoss
      case NonBrokerageRecord.TIME            => time
      case NonBrokerageRecord.LOG_TIME        => logTime
      case _                                  => throw SqlFieldMissing(field)
    }
}
