package rever.etl.data_sync.domain.finance

import rever.etl.data_sync.util.Utils.ImplicitAny
import vn.rever.jdbc.{JdbcRecord, SqlFieldMissing}
object PlatformRecord {

  final val TBL_NAME = "platform_report"

  def field(f: String): String = f
  final val KIND: String = field("data_kind")
  final val PERIOD_TYPE: String = field("period_type")
  final val PERIOD_VALUE: String = field("period_value")
  final val TOOL_AND_SERVER: String = field("tool_and_server")
  final val MARKETING: String = field("marketing")
  final val PERSONNEL: String = field("personnel")
  final val TECH: String = field("tech")
  final val OFFICE_OPERATION: String = field("office_operation")
  final val OTHER_DEPT: String = field("other_dept")
  final val INTEREST_OTHER_GAIN_LOSS: String = field("interest_and_other_gain_loss")
  final val EXPENSE: String = field("expense")
  final val EXPENSE_PER_TOTAL_REVENUE: String = field("expense_per_total_revenue")
  final val TIME: String = field("time")
  final val LOG_TIME: String = field("log_time")

  final val primaryKeys = Seq(KIND, PERIOD_TYPE, PERIOD_VALUE)

  final val fields = Seq(
    KIND,
    PERIOD_TYPE,
    PERIOD_VALUE,
    TOOL_AND_SERVER,
    MARKETING,
    PERSONNEL,
    TECH,
    OFFICE_OPERATION,
    OTHER_DEPT,
    INTEREST_OTHER_GAIN_LOSS,
    EXPENSE,
    EXPENSE_PER_TOTAL_REVENUE,
    TIME,
    LOG_TIME
  )

  def empty: PlatformRecord = {
    PlatformRecord(
      kind = None,
      periodType = None,
      periodValue = None,
      toolAndServer = None,
      marketing = None,
      personnel = None,
      tech = None,
      officeOperation = None,
      otherDept = None,
      interestAndOtherGainLoss = None,
      expense = None,
      expensePerTotalRevenue = None,
      time = None,
      logTime = None
    )
  }
}

case class PlatformRecord(
    var kind: Option[String],
    var periodType: Option[String],
    var periodValue: Option[String],
    var toolAndServer: Option[Double],
    var marketing: Option[Double],
    var personnel: Option[Double],
    var tech: Option[Double],
    var officeOperation: Option[Double],
    var otherDept: Option[Double],
    var interestAndOtherGainLoss: Option[Double] = None,
    var expense: Option[Double],
    var expensePerTotalRevenue: Option[Double] = None,
    var time: Option[Long],
    var logTime: Option[Long]
) extends JdbcRecord {

  override def getPrimaryKeys(): Seq[String] = PlatformRecord.primaryKeys

  override def getFields(): Seq[String] = PlatformRecord.fields

  override def setValues(field: String, value: Any): Unit =
    field match {
      case PlatformRecord.KIND                      => kind = value.asOpt
      case PlatformRecord.PERIOD_TYPE               => periodType = value.asOpt
      case PlatformRecord.PERIOD_VALUE              => periodValue = value.asOpt
      case PlatformRecord.TOOL_AND_SERVER           => toolAndServer = value.asOpt
      case PlatformRecord.MARKETING                 => marketing = value.asOpt
      case PlatformRecord.PERSONNEL                 => personnel = value.asOpt
      case PlatformRecord.TECH                      => tech = value.asOpt
      case PlatformRecord.OFFICE_OPERATION          => officeOperation = value.asOpt
      case PlatformRecord.OTHER_DEPT                => otherDept = value.asOpt
      case PlatformRecord.INTEREST_OTHER_GAIN_LOSS  => interestAndOtherGainLoss = value.asOpt
      case PlatformRecord.EXPENSE                   => expense = value.asOpt
      case PlatformRecord.EXPENSE_PER_TOTAL_REVENUE => expensePerTotalRevenue = value.asOpt
      case PlatformRecord.TIME                      => time = value.asOpt
      case PlatformRecord.LOG_TIME                  => logTime = value.asOpt
      case _                                        => throw SqlFieldMissing(field)
    }

  override def getValue(field: String): Option[Any] =
    field match {
      case PlatformRecord.KIND                      => kind
      case PlatformRecord.PERIOD_TYPE               => periodType
      case PlatformRecord.PERIOD_VALUE              => periodValue
      case PlatformRecord.TOOL_AND_SERVER           => toolAndServer
      case PlatformRecord.MARKETING                 => marketing
      case PlatformRecord.PERSONNEL                 => personnel
      case PlatformRecord.TECH                      => tech
      case PlatformRecord.OFFICE_OPERATION          => officeOperation
      case PlatformRecord.OTHER_DEPT                => otherDept
      case PlatformRecord.INTEREST_OTHER_GAIN_LOSS  => interestAndOtherGainLoss
      case PlatformRecord.EXPENSE                   => expense
      case PlatformRecord.EXPENSE_PER_TOTAL_REVENUE => expensePerTotalRevenue
      case PlatformRecord.TIME                      => time
      case PlatformRecord.LOG_TIME                  => logTime
      case _                                        => throw SqlFieldMissing(field)
    }
}
