package rever.etl.data_sync.domain

import rever.etl.data_sync.domain.HuddleKpiRecord.{FIELDS, PRIMARY_IDS}
import rever.etl.data_sync.jobs.daily_huddle_kpi.HuddleKpiHelper
import vn.rever.common.domain.Implicits.ImplicitAny
import vn.rever.jdbc.{JdbcRecord, SqlFieldMissing}
object HuddleKpiRecord {
  final val INTERVAL_TYPE = "interval_type"
  final val TIME = "time"
  final val TYPE = "type"
  final val DIMENSION = "dimension"
  final val REVENUE = "revenue"
  final val CONNECTED_CALL = "connected_call"
  final val APPOINTMENT = "appointment"
  final val ACTIVE_LISTING = "active_listing"
  final val ACTIVE_INQUIRY = "active_inquiry"
  final val BOOKED_TRANSACTION = "booked_transaction"
  final val TOTAL_AGENT = "total_agent"
  final val AVG_REVENUE_PER_AGENT = "avg_revenue_per_agent"
  final val LOG_TIME = "log_time"

  final val PRIMARY_IDS = Seq(INTERVAL_TYPE, TIME)
  final val FIELDS = Seq(
    INTERVAL_TYPE,
    TIME,
    TYPE,
    DIMENSION,
    REVENUE,
    CONNECTED_CALL,
    APPOINTMENT,
    ACTIVE_LISTING,
    ACTIVE_INQUIRY,
    BOOKED_TRANSACTION,
    TOTAL_AGENT,
    AVG_REVENUE_PER_AGENT,
    LOG_TIME
  )
}
case class HuddleKpiRecord(
    var intervalType: Option[String] = None,
    var time: Option[String] = None,
    var dimension_type: Option[Long] = None,
    var dimension: Option[String] = None,
    var revenue: Option[String] = None,
    var connectedCall: Option[String] = None,
    var meeting: Option[String] = None,
    var activeListing: Option[Long] = None,
    var activeInquiry: Option[String] = None,
    var bookedTransaction: Option[String] = None,
    var totalAgent: Option[Long] = None,
    var avgRevenuePerAgent: Option[String] = None,
    var logTime: Option[String] = None,
) extends JdbcRecord {
  override def getPrimaryKeys(): Seq[String] = PRIMARY_IDS

  override def getFields(): Seq[String] = FIELDS

  override def setValues(field: String, value: Any): Unit = field match {
    case HuddleKpiHelper.INTERVAL_TYPE         => intervalType = value.asOpt
    case HuddleKpiHelper.TIME                  => time = value.asOpt
    case HuddleKpiHelper.TYPE                  => dimension_type = value.asOpt
    case HuddleKpiHelper.DIMENSION             => dimension = value.asOpt
    case HuddleKpiHelper.REVENUE               => revenue = value.asOpt
    case HuddleKpiHelper.CONNECTED_CALL        => connectedCall = value.asOpt
    case HuddleKpiHelper.APPOINTMENT               => meeting = value.asOpt
    case HuddleKpiHelper.ACTIVE_LISTING        => activeListing = value.asOpt
    case HuddleKpiHelper.ACTIVE_INQUIRY        => activeInquiry = value.asOpt
    case HuddleKpiHelper.BOOKED_TRANSACTION        => bookedTransaction = value.asOpt
    case HuddleKpiHelper.TOTAL_AGENT           => totalAgent = value.asOpt
    case HuddleKpiHelper.AVG_REVENUE_PER_AGENT => avgRevenuePerAgent = value.asOpt
    case HuddleKpiHelper.LOG_TIME => logTime = value.asOpt
    case _                                     =>
  }

  override def getValue(field: String): Option[Any] = field match {
    case HuddleKpiHelper.INTERVAL_TYPE         => intervalType
    case HuddleKpiHelper.TIME                  => time
    case HuddleKpiHelper.TYPE                  => dimension_type
    case HuddleKpiHelper.DIMENSION             => dimension
    case HuddleKpiHelper.REVENUE               => revenue
    case HuddleKpiHelper.CONNECTED_CALL        => connectedCall
    case HuddleKpiHelper.APPOINTMENT               => meeting
    case HuddleKpiHelper.ACTIVE_LISTING        => activeListing
    case HuddleKpiHelper.ACTIVE_INQUIRY        => activeInquiry
    case HuddleKpiHelper.BOOKED_TRANSACTION        => bookedTransaction
    case HuddleKpiHelper.TOTAL_AGENT           => totalAgent
    case HuddleKpiHelper.AVG_REVENUE_PER_AGENT => avgRevenuePerAgent
    case HuddleKpiHelper.LOG_TIME => logTime
    case _                                     => throw SqlFieldMissing(field)
  }

  def toMap: Map[String, Any] = {
    FIELDS
      .map(field => field -> getValue(field).get)
      .toMap
  }
}
