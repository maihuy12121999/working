package rever.etl.data_sync.domain.finance
import rever.etl.data_sync.util.Utils.ImplicitAny
import vn.rever.jdbc.{JdbcRecord, SqlFieldMissing}
object CostRecord {

  final val TBL_NAME = "cost_report"

  def field(f: String): String = f

  final val KIND: String = field("data_kind")
  final val PERIOD_TYPE: String = field("period_type")
  final val PERIOD_VALUE: String = field("period_value")
  final val SALE_PRIMARY_PERSONNEL: String = field("sale_primary_personnel")
  final val SALE_SECONDARY_PERSONNEL: String = field("sale_secondary_personnel")
  final val TECH_PERSONNEL: String = field("tech_personnel")
  final val OTHER_DEPT_PERSONNEL: String = field("other_dept_personnel")
  final val RECRUITMENT_PERSONNEL: String = field("recruitment_personnel")
  final val RENTAL_TRANSACTION_CENTER_OFFICE: String = field("rental_transaction_center_office")
  final val RENTAL_HEAD_OFFICE: String = field("rental_head_office")
  final val OPERATION_OFFICE: String = field("operation_office")
  final val DEPRECIATION_OFFICE: String = field("depreciation_office")
  final val TOOL_AND_SERVER: String = field("tool_and_server")
  final val OTHER_GAIN_LOSS: String = field("other_gain_loss")
  final val TOTAL: String = field("total")
  final val TIME: String = field("time")
  final val LOG_TIME: String = field("log_time")

  final val primaryKeys = Seq(KIND, PERIOD_TYPE, PERIOD_VALUE)

  final val fields = Seq(
    KIND,
    PERIOD_TYPE,
    PERIOD_VALUE,
    SALE_PRIMARY_PERSONNEL,
    SALE_SECONDARY_PERSONNEL,
    TECH_PERSONNEL,
    OTHER_DEPT_PERSONNEL,
    RECRUITMENT_PERSONNEL,
    RENTAL_TRANSACTION_CENTER_OFFICE,
    RENTAL_HEAD_OFFICE,
    OPERATION_OFFICE,
    DEPRECIATION_OFFICE,
    TOOL_AND_SERVER,
    OTHER_GAIN_LOSS,
    TOTAL,
    TIME,
    LOG_TIME
  )

  def empty: CostRecord = {
    CostRecord(
      kind = None,
      periodType = None,
      periodValue = None,
      salePrimaryPersonnel = None,
      saleSecondaryPersonnel = None,
      techPersonnel = None,
      otherDeptPersonnel = None,
      recruitmentPersonnel = None,
      rentalTransactionCenterOffice = None,
      rentalHeadOffice = None,
      operationOffice = None,
      depreciationOffice = None,
      toolAndServer = None,
      otherGainLoss = None,
      total = None,
      time = None,
      logTime = None
    )
  }
}

case class CostRecord(
    var kind: Option[String],
    var periodType: Option[String],
    var periodValue: Option[String],
    var salePrimaryPersonnel: Option[Double],
    var saleSecondaryPersonnel: Option[Double],
    var techPersonnel: Option[Double],
    var otherDeptPersonnel: Option[Double],
    var recruitmentPersonnel: Option[Double],
    var rentalTransactionCenterOffice: Option[Double],
    var rentalHeadOffice: Option[Double] = None,
    var operationOffice: Option[Double] = None,
    var depreciationOffice: Option[Double],
    var toolAndServer: Option[Double],
    var otherGainLoss: Option[Double] = None,
    var total: Option[Double] = None,
    var time: Option[Long],
    var logTime: Option[Long]
) extends JdbcRecord {

  override def getPrimaryKeys(): Seq[String] = CostRecord.primaryKeys

  override def getFields(): Seq[String] = CostRecord.fields

  override def setValues(field: String, value: Any): Unit =
    field match {
      case CostRecord.KIND                             => kind = value.asOpt
      case CostRecord.PERIOD_TYPE                      => periodType = value.asOpt
      case CostRecord.PERIOD_VALUE                     => periodValue = value.asOpt
      case CostRecord.SALE_PRIMARY_PERSONNEL           => salePrimaryPersonnel = value.asOpt
      case CostRecord.SALE_SECONDARY_PERSONNEL         => saleSecondaryPersonnel = value.asOpt
      case CostRecord.TECH_PERSONNEL                   => techPersonnel = value.asOpt
      case CostRecord.OTHER_DEPT_PERSONNEL             => otherDeptPersonnel = value.asOpt
      case CostRecord.RECRUITMENT_PERSONNEL            => recruitmentPersonnel = value.asOpt
      case CostRecord.RENTAL_TRANSACTION_CENTER_OFFICE => rentalTransactionCenterOffice = value.asOpt
      case CostRecord.RENTAL_HEAD_OFFICE               => rentalHeadOffice = value.asOpt
      case CostRecord.OPERATION_OFFICE                 => operationOffice = value.asOpt
      case CostRecord.DEPRECIATION_OFFICE              => depreciationOffice = value.asOpt
      case CostRecord.TOOL_AND_SERVER                  => toolAndServer = value.asOpt
      case CostRecord.OTHER_GAIN_LOSS                  => otherGainLoss = value.asOpt
      case CostRecord.TOTAL                            => total = value.asOpt
      case CostRecord.TIME                             => time = value.asOpt
      case CostRecord.LOG_TIME                         => logTime = value.asOpt
      case _                                           => throw SqlFieldMissing(field)
    }

  override def getValue(field: String): Option[Any] =
    field match {
      case CostRecord.KIND                             => kind
      case CostRecord.PERIOD_TYPE                      => periodType
      case CostRecord.PERIOD_VALUE                     => periodValue
      case CostRecord.SALE_PRIMARY_PERSONNEL           => salePrimaryPersonnel
      case CostRecord.SALE_SECONDARY_PERSONNEL         => saleSecondaryPersonnel
      case CostRecord.TECH_PERSONNEL                   => techPersonnel
      case CostRecord.OTHER_DEPT_PERSONNEL             => otherDeptPersonnel
      case CostRecord.RECRUITMENT_PERSONNEL            => recruitmentPersonnel
      case CostRecord.RENTAL_TRANSACTION_CENTER_OFFICE => rentalTransactionCenterOffice
      case CostRecord.RENTAL_HEAD_OFFICE               => rentalHeadOffice
      case CostRecord.OPERATION_OFFICE                 => operationOffice
      case CostRecord.DEPRECIATION_OFFICE              => depreciationOffice
      case CostRecord.TOOL_AND_SERVER                  => toolAndServer
      case CostRecord.OTHER_GAIN_LOSS                  => otherGainLoss
      case CostRecord.TOTAL                            => total
      case CostRecord.TIME                             => time
      case CostRecord.LOG_TIME                         => logTime
      case _                                           => throw SqlFieldMissing(field)
    }
}
