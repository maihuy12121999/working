package rever.etl.data_sync.domain.finance

import rever.etl.data_sync.util.Utils.ImplicitAny
import vn.rever.jdbc.{JdbcRecord, SqlFieldMissing}

object AgentLevelReport {

  final val TBL_NAME = "by_levels"

  def field(f: String): String = f
  final val KIND: String = field("data_kind")
  final val PERIOD_TYPE: String = field("period_type")
  final val PERIOD_VALUE: String = field("period_value")
  final val BUSINESS_UNIT: String = field("business_unit")
  final val SERVICE_TYPE: String = field("service_type")
  final val CATEGORY_LEVEL: String = field("category_level")
  final val REVENUE: String = field("revenue")
  final val TRANSACTION: String = field("transaction")
  final val COMMISSION: String = field("commission")
  final val TIME: String = field("time")
  final val LOG_TIME: String = field("log_time")

  final val primaryKeys = Seq(KIND, PERIOD_TYPE, PERIOD_VALUE)

  final val fields = Seq(
    KIND,
    PERIOD_TYPE,
    PERIOD_VALUE,
    BUSINESS_UNIT,
    SERVICE_TYPE,
    CATEGORY_LEVEL,
    REVENUE,
    TRANSACTION,
    COMMISSION,
    TIME,
    LOG_TIME
  )

  def empty: AgentLevelReport = {
    AgentLevelReport(
      kind = None,
      periodType = None,
      periodValue = None,
      businessUnit = None,
      serviceType = None,
      categoryLevel = None,
      transaction = None,
      revenue = None,
      commission = None,
      time = None,
      logTime = None
    )
  }
}

case class AgentLevelReport(
    var kind: Option[String],
    var periodType: Option[String],
    var periodValue: Option[String],
    var businessUnit: Option[String],
    var serviceType: Option[String],
    var categoryLevel: Option[String],
    var transaction: Option[Int],
    var revenue: Option[Double],
    var commission: Option[Double],
    var time: Option[Long],
    var logTime: Option[Long]
) extends JdbcRecord {

  override def getPrimaryKeys(): Seq[String] = AgentLevelReport.primaryKeys

  override def getFields(): Seq[String] = AgentLevelReport.fields

  override def setValues(field: String, value: Any): Unit =
    field match {
      case AgentLevelReport.KIND           => kind = value.asOpt
      case AgentLevelReport.PERIOD_TYPE    => periodType = value.asOpt
      case AgentLevelReport.PERIOD_VALUE   => periodValue = value.asOpt
      case AgentLevelReport.BUSINESS_UNIT  => businessUnit = value.asOpt
      case AgentLevelReport.SERVICE_TYPE   => serviceType = value.asOpt
      case AgentLevelReport.CATEGORY_LEVEL => categoryLevel = value.asOpt
      case AgentLevelReport.TRANSACTION    => transaction = value.asOpt
      case AgentLevelReport.REVENUE        => revenue = value.asOpt
      case AgentLevelReport.COMMISSION     => commission = value.asOpt
      case AgentLevelReport.TIME           => time = value.asOpt
      case AgentLevelReport.LOG_TIME       => logTime = value.asOpt
      case _                               => throw SqlFieldMissing(field)
    }

  override def getValue(field: String): Option[Any] =
    field match {
      case AgentLevelReport.KIND           => kind
      case AgentLevelReport.PERIOD_TYPE    => periodType
      case AgentLevelReport.PERIOD_VALUE   => periodValue
      case AgentLevelReport.BUSINESS_UNIT  => businessUnit
      case AgentLevelReport.SERVICE_TYPE   => serviceType
      case AgentLevelReport.CATEGORY_LEVEL => categoryLevel
      case AgentLevelReport.TRANSACTION    => transaction
      case AgentLevelReport.REVENUE        => revenue
      case AgentLevelReport.COMMISSION     => commission
      case AgentLevelReport.TIME           => time
      case AgentLevelReport.LOG_TIME       => logTime
      case _                               => throw SqlFieldMissing(field)
    }
}
