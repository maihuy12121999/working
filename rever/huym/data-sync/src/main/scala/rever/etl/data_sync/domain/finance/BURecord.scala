package rever.etl.data_sync.domain.finance
import rever.etl.data_sync.util.Utils.ImplicitAny
import vn.rever.jdbc.{JdbcRecord, SqlFieldMissing}
object BURecord {

  final val TBL_NAME = "bu_brokerage_report"

  def field(f: String): String = f

  final val KIND: String = field("data_kind")
  final val DIMENSION: String = field("dimension")
  final val PERIOD_TYPE: String = field("period_type")
  final val PERIOD_VALUE: String = field("period_value")
  final val NUM_OF_TRANSACTIONS: String = field("num_of_transactions")
  final val GROSS_TRANSACTION_VALUE: String = field("gross_transaction_value")
  final val REVENUE: String = field("revenue")
  final val COMMISSION: String = field("commission")
  final val GROSS_PROFIT: String = field("gross_profit")
  final val SELLING: String = field("cos")
  final val MARKETING: String = field("marketing")
  final val COS_PER_REVENUE: String = field("cos_per_revenue")
  final val MARKETING_PER_REVENUE: String = field("marketing_per_revenue")
  final val PERSONNEL: String = field("personnel")
  final val RENTAL: String = field("rental")
  final val OFFICE_OPERATION: String = field("office_operation")
  final val PERSONNEL_PER_REVENUE: String = field("personnel_per_revenue")
  final val RENTAL_PER_REVENUE: String = field("rental_per_revenue")
  final val OFFICE_OPERATION_PER_REVENUE: String = field("office_operation_per_revenue")
  final val PROFIT_AND_LOSS: String = field("profit_and_loss")
  final val PROFIT_AND_LOSS_PER_REVENUE: String = field("profit_and_loss_per_revenue")
  final val TIME: String = field("time")
  final val LOG_TIME: String = field("log_time")

  final val primaryKeys = Seq(KIND, DIMENSION, PERIOD_TYPE, PERIOD_VALUE)

  final val fields = Seq(
    KIND,
    DIMENSION,
    PERIOD_TYPE,
    PERIOD_VALUE,
    NUM_OF_TRANSACTIONS,
    GROSS_TRANSACTION_VALUE,
    REVENUE,
    COMMISSION,
//    GROSS_PROFIT,
    SELLING,
    MARKETING,
    COS_PER_REVENUE,
    MARKETING_PER_REVENUE,
    PERSONNEL,
    RENTAL,
    OFFICE_OPERATION,
    PERSONNEL_PER_REVENUE,
    RENTAL_PER_REVENUE,
    OFFICE_OPERATION_PER_REVENUE,
    PROFIT_AND_LOSS,
    PROFIT_AND_LOSS_PER_REVENUE,
    TIME,
    LOG_TIME
  )

  def empty: BURecord = {
    BURecord(
      kind = None,
      dimension = None,
      periodType = None,
      periodValue = None,
      numOfTransactions = None,
      grossTransactionValue = None,
      revenue = None,
      commission = None,
      grossProfit = None,
      selling = None,
      marketing = None,
      cosPerRevenue = None,
      marketingPerRevenue = None,
      personnel = None,
      rental = None,
      officeOperation = None,
      personnelPerRevenue = None,
      rentalPerRevenue = None,
      officeOperationPerRevenue = None,
      profitAndLoss = None,
      profitAndLossPerRevenue = None,
      time = None,
      logTime = None
    )
  }
}

case class BURecord(
    var kind: Option[String],
    var dimension: Option[String],
    var periodType: Option[String],
    var periodValue: Option[String],
    var numOfTransactions: Option[Int],
    var grossTransactionValue: Option[Double],
    var revenue: Option[Double],
    var commission: Option[Double],
    var grossProfit: Option[Double],
    var selling: Option[Double],
    var marketing: Option[Double],
    var cosPerRevenue: Option[Double] = None,
    var marketingPerRevenue: Option[Double] = None,
    var personnel: Option[Double],
    var rental: Option[Double],
    var officeOperation: Option[Double],
    var personnelPerRevenue: Option[Double] = None,
    var rentalPerRevenue: Option[Double] = None,
    var officeOperationPerRevenue: Option[Double] = None,
    var profitAndLoss: Option[Double],
    var profitAndLossPerRevenue: Option[Double] = None,
    var time: Option[Long],
    var logTime: Option[Long]
) extends JdbcRecord {

  override def getPrimaryKeys(): Seq[String] = BURecord.primaryKeys

  override def getFields(): Seq[String] = BURecord.fields

  override def setValues(field: String, value: Any): Unit =
    field match {
      case BURecord.KIND                         => kind = value.asOpt
      case BURecord.DIMENSION                    => dimension = value.asOpt
      case BURecord.PERIOD_TYPE                  => periodType = value.asOpt
      case BURecord.PERIOD_VALUE                 => periodValue = value.asOpt
      case BURecord.NUM_OF_TRANSACTIONS          => numOfTransactions = value.asOpt
      case BURecord.GROSS_TRANSACTION_VALUE      => grossTransactionValue = value.asOpt
      case BURecord.REVENUE                      => revenue = value.asOpt
      case BURecord.COMMISSION                   => commission = value.asOpt
      case BURecord.GROSS_PROFIT                 => grossProfit = value.asOpt
      case BURecord.SELLING                      => selling = value.asOpt
      case BURecord.MARKETING                    => marketing = value.asOpt
      case BURecord.COS_PER_REVENUE              => cosPerRevenue = value.asOpt
      case BURecord.MARKETING_PER_REVENUE        => marketingPerRevenue = value.asOpt
      case BURecord.PERSONNEL                    => personnel = value.asOpt
      case BURecord.RENTAL                       => rental = value.asOpt
      case BURecord.OFFICE_OPERATION             => officeOperation = value.asOpt
      case BURecord.PERSONNEL_PER_REVENUE        => personnelPerRevenue = value.asOpt
      case BURecord.RENTAL_PER_REVENUE           => rentalPerRevenue = value.asOpt
      case BURecord.OFFICE_OPERATION_PER_REVENUE => officeOperationPerRevenue = value.asOpt
      case BURecord.PROFIT_AND_LOSS              => profitAndLoss = value.asOpt
      case BURecord.PROFIT_AND_LOSS_PER_REVENUE  => profitAndLossPerRevenue = value.asOpt
      case BURecord.TIME                         => time = value.asOpt
      case BURecord.LOG_TIME                     => logTime = value.asOpt
      case _                                     => throw SqlFieldMissing(field)
    }

  override def getValue(field: String): Option[Any] =
    field match {
      case BURecord.KIND                         => kind
      case BURecord.DIMENSION                    => dimension
      case BURecord.PERIOD_TYPE                  => periodType
      case BURecord.PERIOD_VALUE                 => periodValue
      case BURecord.NUM_OF_TRANSACTIONS          => numOfTransactions
      case BURecord.GROSS_TRANSACTION_VALUE      => grossTransactionValue
      case BURecord.REVENUE                      => revenue
      case BURecord.COMMISSION                   => commission
      case BURecord.GROSS_PROFIT                 => grossProfit
      case BURecord.SELLING                      => selling
      case BURecord.MARKETING                    => marketing
      case BURecord.COS_PER_REVENUE              => cosPerRevenue
      case BURecord.MARKETING_PER_REVENUE        => marketingPerRevenue
      case BURecord.PERSONNEL                    => personnel
      case BURecord.RENTAL                       => rental
      case BURecord.OFFICE_OPERATION             => officeOperation
      case BURecord.PERSONNEL_PER_REVENUE        => personnelPerRevenue
      case BURecord.RENTAL_PER_REVENUE           => rentalPerRevenue
      case BURecord.OFFICE_OPERATION_PER_REVENUE => officeOperationPerRevenue
      case BURecord.PROFIT_AND_LOSS              => profitAndLoss
      case BURecord.PROFIT_AND_LOSS_PER_REVENUE  => profitAndLossPerRevenue
      case BURecord.TIME                         => time
      case BURecord.LOG_TIME                     => logTime
      case _                                     => throw SqlFieldMissing(field)
    }
}
