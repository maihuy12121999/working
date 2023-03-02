package rever.etl.data_sync.domain.finance

import rever.etl.data_sync.util.Utils.ImplicitAny
import vn.rever.jdbc.{JdbcRecord, SqlFieldMissing}

object BrokerageRecord {

  final val TBL_NAME = "brokerage_report"

  def field(f: String): String = f

  final val KIND: String = field("data_kind")
  final val DIMENSION: String = field("dimension")
  final val PERIOD_TYPE: String = field("period_type")
  final val PERIOD_VALUE: String = field("period_value")
  final val NUM_OF_TRANSACTIONS: String = field("num_of_transactions")
  final val GROSS_TRANSACTION_VALUE: String = field("gross_transaction_value")
  final val REVENUE: String = field("revenue")
  final val COS: String = field("cos")
  final val GROSS_PROFIT: String = field("gross_profit")
  final val GROSS_PROFIT_PER_REVENUE: String = field("gross_profit_per_revenue")
  final val SELLING_AND_MARKETING: String = field("selling_and_marketing")
  final val SELLING_AND_MARKETING_PER_REVENUE: String = field("selling_and_marketing_per_revenue")
  final val PERSONNEL_OFFICE_OPERATION: String = field("personnel_office_operation")
  final val PERSONNEL_OFFICE_OPERATION_PER_REVENUE: String = field("personnel_office_operation_per_revenue")
  final val PROFIT_AND_LOSS: String = field("profit_and_loss")
  final val PROFIT_AND_LOSS_PER_REVENUE: String = field("profit_and_loss_per_revenue")
  final val PROFIT_AND_LOSS_BEFORE_TECH: String = field("profit_and_loss_before_tech")
  final val PROFIT_AND_LOSS_BEFORE_TECH_PER_REVENUE: String = field("profit_and_loss_before_tech_per_revenue")
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
    COS,
    GROSS_PROFIT,
    GROSS_PROFIT_PER_REVENUE,
    SELLING_AND_MARKETING,
    SELLING_AND_MARKETING_PER_REVENUE,
    PERSONNEL_OFFICE_OPERATION,
    PERSONNEL_OFFICE_OPERATION_PER_REVENUE,
    PROFIT_AND_LOSS,
    PROFIT_AND_LOSS_PER_REVENUE,
    PROFIT_AND_LOSS_BEFORE_TECH,
    PROFIT_AND_LOSS_BEFORE_TECH_PER_REVENUE,
    TIME,
    LOG_TIME
  )

  def empty: BrokerageRecord = {
    BrokerageRecord(
      kind = None,
      dimension = None,
      periodType = None,
      periodValue = None,
      numOfTransactions = None,
      grossTransactionValue = None,
      revenue = None,
      cog = None,
      grossProfit = None,
      grossProfitPerRevenue = None,
      sellingAndMarketing = None,
      sellingAndMarketingPerRevenue = None,
      personnelOfficeOperation = None,
      personnelOfficeOperationPerRevenue = None,
      profitAndLoss = None,
      profitAndLossPerRevenue = None,
      profitAndLossBeforeTech = None,
      profitAndLossBeforeTechPerRevenue = None,
      time = None,
      logTime = None
    )
  }
}

case class BrokerageRecord(
    var kind: Option[String],
    var dimension: Option[String],
    var periodType: Option[String],
    var periodValue: Option[String],
    var numOfTransactions: Option[Int],
    var grossTransactionValue: Option[Double],
    var revenue: Option[Double],
    var cog: Option[Double],
    var grossProfit: Option[Double],
    var grossProfitPerRevenue: Option[Double],
    var sellingAndMarketing: Option[Double],
    var sellingAndMarketingPerRevenue: Option[Double] = None,
    var personnelOfficeOperation: Option[Double],
    var personnelOfficeOperationPerRevenue: Option[Double] = None,
    var profitAndLoss: Option[Double],
    var profitAndLossPerRevenue: Option[Double] = None,
    var profitAndLossBeforeTech: Option[Double] = None,
    var profitAndLossBeforeTechPerRevenue: Option[Double] = None,
    var time: Option[Long],
    var logTime: Option[Long]
) extends JdbcRecord {

  override def getPrimaryKeys(): Seq[String] = BrokerageRecord.primaryKeys

  override def getFields(): Seq[String] = BrokerageRecord.fields

  override def setValues(field: String, value: Any): Unit =
    field match {
      case BrokerageRecord.KIND                                   => kind = value.asOpt
      case BrokerageRecord.DIMENSION                              => dimension = value.asOpt
      case BrokerageRecord.PERIOD_TYPE                            => periodType = value.asOpt
      case BrokerageRecord.PERIOD_VALUE                           => periodValue = value.asOpt
      case BrokerageRecord.NUM_OF_TRANSACTIONS                    => numOfTransactions = value.asOpt
      case BrokerageRecord.GROSS_TRANSACTION_VALUE                => grossTransactionValue = value.asOpt
      case BrokerageRecord.REVENUE                                => revenue = value.asOpt
      case BrokerageRecord.COS                                    => cog = value.asOpt
      case BrokerageRecord.GROSS_PROFIT                           => grossProfit = value.asOpt
      case BrokerageRecord.GROSS_PROFIT_PER_REVENUE               => grossProfitPerRevenue = value.asOpt
      case BrokerageRecord.SELLING_AND_MARKETING                  => sellingAndMarketing = value.asOpt
      case BrokerageRecord.SELLING_AND_MARKETING_PER_REVENUE      => sellingAndMarketingPerRevenue = value.asOpt
      case BrokerageRecord.PERSONNEL_OFFICE_OPERATION             => personnelOfficeOperation = value.asOpt
      case BrokerageRecord.PERSONNEL_OFFICE_OPERATION_PER_REVENUE => personnelOfficeOperationPerRevenue = value.asOpt
      case BrokerageRecord.PROFIT_AND_LOSS                        => profitAndLoss = value.asOpt
      case BrokerageRecord.PROFIT_AND_LOSS_PER_REVENUE            => profitAndLossPerRevenue = value.asOpt
      case BrokerageRecord.PROFIT_AND_LOSS_BEFORE_TECH            => profitAndLossBeforeTech = value.asOpt
      case BrokerageRecord.PROFIT_AND_LOSS_BEFORE_TECH_PER_REVENUE =>
        profitAndLossBeforeTechPerRevenue = value.asOpt
      case BrokerageRecord.TIME     => time = value.asOpt
      case BrokerageRecord.LOG_TIME => logTime = value.asOpt
      case _                        => throw SqlFieldMissing(field)
    }

  override def getValue(field: String): Option[Any] =
    field match {
      case BrokerageRecord.KIND                                    => kind
      case BrokerageRecord.DIMENSION                               => dimension
      case BrokerageRecord.PERIOD_TYPE                             => periodType
      case BrokerageRecord.PERIOD_VALUE                            => periodValue
      case BrokerageRecord.NUM_OF_TRANSACTIONS                     => numOfTransactions
      case BrokerageRecord.GROSS_TRANSACTION_VALUE                 => grossTransactionValue
      case BrokerageRecord.REVENUE                                 => revenue
      case BrokerageRecord.COS                                     => cog
      case BrokerageRecord.GROSS_PROFIT                            => grossProfit
      case BrokerageRecord.GROSS_PROFIT_PER_REVENUE                => grossProfitPerRevenue
      case BrokerageRecord.SELLING_AND_MARKETING                   => sellingAndMarketing
      case BrokerageRecord.SELLING_AND_MARKETING_PER_REVENUE       => sellingAndMarketingPerRevenue
      case BrokerageRecord.PERSONNEL_OFFICE_OPERATION              => personnelOfficeOperation
      case BrokerageRecord.PERSONNEL_OFFICE_OPERATION_PER_REVENUE  => personnelOfficeOperationPerRevenue
      case BrokerageRecord.PROFIT_AND_LOSS                         => profitAndLoss
      case BrokerageRecord.PROFIT_AND_LOSS_PER_REVENUE             => profitAndLossPerRevenue
      case BrokerageRecord.PROFIT_AND_LOSS_BEFORE_TECH             => profitAndLossBeforeTech
      case BrokerageRecord.PROFIT_AND_LOSS_BEFORE_TECH_PER_REVENUE => profitAndLossBeforeTechPerRevenue
      case BrokerageRecord.TIME                                    => time
      case BrokerageRecord.LOG_TIME                                => logTime
      case _                                                       => throw SqlFieldMissing(field)
    }
}
