package rever.etl.data_sync.domain.finance
import rever.etl.data_sync.util.Utils.ImplicitAny
import vn.rever.jdbc.{JdbcRecord, SqlFieldMissing}

object DealRecord {

  final val TBL_NAME = "deal_brokerage_report"

  def field(f: String): String = f

  final val BUSINESS_UNIT: String = field("business_unit")
  final val DEAL_STATE: String = field("deal_state")
  final val PERIOD_TYPE: String = field("period_type")
  final val PERIOD_VALUE: String = field("period_value")
  final val TOTAL_TRANSACTIONS: String = field("total_transactions")
  final val SALE_TRANSACTIONS: String = field("sale_transactions")
  final val LEASE_TRANSACTIONS: String = field("lease_transactions")
  final val TOTAL_REVENUE: String = field("total_revenue")
  final val SALE_REVENUE: String = field("sale_revenue")
  final val LEASE_REVENUE: String = field("lease_revenue")
  final val TIME: String = field("time")
  final val LOG_TIME: String = field("log_time")

  final val primaryKeys = Seq(BUSINESS_UNIT, DEAL_STATE, PERIOD_TYPE, PERIOD_VALUE)

  final val fields = Seq(
    BUSINESS_UNIT,
    DEAL_STATE,
    PERIOD_TYPE,
    PERIOD_VALUE,
    TOTAL_TRANSACTIONS,
    SALE_TRANSACTIONS,
    LEASE_TRANSACTIONS,
    TOTAL_REVENUE,
    SALE_REVENUE,
    LEASE_REVENUE,
    TIME,
    LOG_TIME
  )

  def empty: DealRecord = {
    DealRecord(
      businessUnit = None,
      dealState = None,
      periodType = None,
      periodValue = None,
      totalTransactions = None,
      saleTransactions = None,
      leaseTransactions = None,
      totalRevenue = None,
      saleRevenue = None,
      leaseRevenue = None,
      time = None,
      logTime = None
    )
  }
}

case class DealRecord(
    var businessUnit: Option[String],
    var dealState: Option[String],
    var periodType: Option[String],
    var periodValue: Option[String],
    var totalTransactions: Option[Int],
    var saleTransactions: Option[Double],
    var leaseTransactions: Option[Double],
    var totalRevenue: Option[Double],
    var saleRevenue: Option[Double],
    var leaseRevenue: Option[Double],
    var time: Option[Long],
    var logTime: Option[Long]
) extends JdbcRecord {

  override def getPrimaryKeys(): Seq[String] = DealRecord.primaryKeys

  override def getFields(): Seq[String] = DealRecord.fields

  override def setValues(field: String, value: Any): Unit =
    field match {
      case DealRecord.BUSINESS_UNIT      => businessUnit = value.asOpt
      case DealRecord.DEAL_STATE         => dealState = value.asOpt
      case DealRecord.PERIOD_TYPE        => periodType = value.asOpt
      case DealRecord.PERIOD_VALUE       => periodValue = value.asOpt
      case DealRecord.TOTAL_TRANSACTIONS => totalTransactions = value.asOpt
      case DealRecord.SALE_TRANSACTIONS  => saleTransactions = value.asOpt
      case DealRecord.LEASE_TRANSACTIONS => leaseTransactions = value.asOpt
      case DealRecord.TOTAL_REVENUE      => totalRevenue = value.asOpt
      case DealRecord.SALE_REVENUE       => saleRevenue = value.asOpt
      case DealRecord.LEASE_REVENUE      => leaseRevenue = value.asOpt
      case DealRecord.TIME               => time = value.asOpt
      case DealRecord.LOG_TIME           => logTime = value.asOpt
      case _                             => throw SqlFieldMissing(field)
    }

  override def getValue(field: String): Option[Any] =
    field match {
      case DealRecord.BUSINESS_UNIT      => businessUnit
      case DealRecord.DEAL_STATE         => dealState
      case DealRecord.PERIOD_TYPE        => periodType
      case DealRecord.PERIOD_VALUE       => periodValue
      case DealRecord.TOTAL_TRANSACTIONS => totalTransactions
      case DealRecord.SALE_TRANSACTIONS  => saleTransactions
      case DealRecord.LEASE_TRANSACTIONS => leaseTransactions
      case DealRecord.TOTAL_REVENUE      => totalRevenue
      case DealRecord.SALE_REVENUE       => saleRevenue
      case DealRecord.LEASE_REVENUE      => leaseRevenue
      case DealRecord.TIME               => time
      case DealRecord.LOG_TIME           => logTime
      case _                             => throw SqlFieldMissing(field)
    }
}
