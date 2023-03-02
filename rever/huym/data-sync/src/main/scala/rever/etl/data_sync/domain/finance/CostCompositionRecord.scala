package rever.etl.data_sync.domain.finance

import rever.etl.data_sync.util.Utils.ImplicitAny
import vn.rever.jdbc.{JdbcRecord, SqlFieldMissing}

object CostCompositionRecord {

  final val TBL_NAME = "cost_composition_report"

  def field(f: String): String = f

  final val KIND: String = field("data_kind")
  final val PERIOD_TYPE: String = field("period_type")
  final val PERIOD_VALUE: String = field("period_value")
  final val BUSINESS_UNIT: String = field("business_unit")
  final val REVENUE: String = field("revenue")
  final val OFFICE: String = field("office")
  final val DEPRECIATION: String = field("depreciation")
  final val TOOL_AND_SERVER: String = field("tool_and_server")
  final val SERVICE_FEE: String = field("service_fee")
  final val TOTAL_COST: String = field("total_cost")
  final val TIME: String = field("time")
  final val LOG_TIME: String = field("log_time")

  final val primaryKeys = Seq(KIND, PERIOD_TYPE, PERIOD_VALUE)

  final val fields = Seq(
    KIND,
    PERIOD_TYPE,
    PERIOD_VALUE,
    BUSINESS_UNIT,
    REVENUE,
    OFFICE,
    DEPRECIATION,
    TOOL_AND_SERVER,
    SERVICE_FEE,
    TOTAL_COST,
    TIME,
    LOG_TIME
  )

  def empty: CostCompositionRecord = {
    CostCompositionRecord(
      kind = None,
      periodType = None,
      periodValue = None,
      businessUnit = None,
      revenue = None,
      office = None,
      depreciation = None,
      toolAndServer = None,
      serviceFee = None,
      totalCost = None,
      time = None,
      logTime = None
    )
  }
}

case class CostCompositionRecord(
    var kind: Option[String],
    var periodType: Option[String],
    var periodValue: Option[String],
    var businessUnit: Option[String],
    var revenue: Option[Double],
    var office: Option[Double],
    var depreciation: Option[Double],
    var toolAndServer: Option[Double],
    var serviceFee: Option[Double],
    var totalCost: Option[Double],
    var time: Option[Long],
    var logTime: Option[Long]
) extends JdbcRecord {

  override def getPrimaryKeys(): Seq[String] = CostCompositionRecord.primaryKeys

  override def getFields(): Seq[String] = CostCompositionRecord.fields

  override def setValues(field: String, value: Any): Unit = {
    field match {
      case CostCompositionRecord.KIND            => kind = value.asOpt
      case CostCompositionRecord.PERIOD_TYPE     => periodType = value.asOpt
      case CostCompositionRecord.PERIOD_VALUE    => periodValue = value.asOpt
      case CostCompositionRecord.BUSINESS_UNIT   => businessUnit = value.asOpt
      case CostCompositionRecord.REVENUE         => revenue = value.asOpt
      case CostCompositionRecord.OFFICE          => office = value.asOpt
      case CostCompositionRecord.DEPRECIATION    => depreciation = value.asOpt
      case CostCompositionRecord.TOOL_AND_SERVER => toolAndServer = value.asOpt
      case CostCompositionRecord.SERVICE_FEE     => serviceFee = value.asOpt
      case CostCompositionRecord.TOTAL_COST      => totalCost = value.asOpt
      case CostCompositionRecord.TIME            => time = value.asOpt
      case CostCompositionRecord.LOG_TIME        => logTime = value.asOpt
      case _                                     => throw SqlFieldMissing(field)
    }
  }

  override def getValue(field: String): Option[Any] = {
    field match {
      case CostCompositionRecord.KIND            => kind
      case CostCompositionRecord.PERIOD_TYPE     => periodType
      case CostCompositionRecord.PERIOD_VALUE    => periodValue
      case CostCompositionRecord.BUSINESS_UNIT   => businessUnit
      case CostCompositionRecord.REVENUE         => revenue
      case CostCompositionRecord.OFFICE          => office
      case CostCompositionRecord.DEPRECIATION    => depreciation
      case CostCompositionRecord.TOOL_AND_SERVER => toolAndServer
      case CostCompositionRecord.SERVICE_FEE     => serviceFee
      case CostCompositionRecord.TOTAL_COST      => totalCost
      case CostCompositionRecord.TIME            => time
      case CostCompositionRecord.LOG_TIME        => logTime
      case _                                     => throw SqlFieldMissing(field)
    }
  }
}
