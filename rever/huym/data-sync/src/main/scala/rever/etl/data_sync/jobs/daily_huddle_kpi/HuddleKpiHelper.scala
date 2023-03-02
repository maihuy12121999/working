package rever.etl.data_sync.jobs.daily_huddle_kpi

import org.apache.spark.sql.Row

object HuddleKpiHelper {
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

  def toHuddleKpiRecord(row: Row): Map[String,Any] = {
    Map[String, Any](
      HuddleKpiHelper.INTERVAL_TYPE -> row.getAs[String](INTERVAL_TYPE),
      HuddleKpiHelper.TIME -> row.getAs[Long](TIME),
      HuddleKpiHelper.TYPE -> row.getAs[String](TYPE),
      HuddleKpiHelper.DIMENSION -> row.getAs[String](DIMENSION),
      HuddleKpiHelper.REVENUE -> row.getAs[Double](REVENUE),
      HuddleKpiHelper.CONNECTED_CALL -> row.getAs[Long](CONNECTED_CALL),
      HuddleKpiHelper.APPOINTMENT -> row.getAs[Long](APPOINTMENT),
      HuddleKpiHelper.ACTIVE_LISTING -> row.getAs[Long](ACTIVE_LISTING),
      HuddleKpiHelper.ACTIVE_INQUIRY -> row.getAs[Long](ACTIVE_INQUIRY),
      HuddleKpiHelper.BOOKED_TRANSACTION -> row.getAs[Long](BOOKED_TRANSACTION),
      HuddleKpiHelper.TOTAL_AGENT -> row.getAs[Long](TOTAL_AGENT),
      HuddleKpiHelper.AVG_REVENUE_PER_AGENT -> row.getAs[Double](AVG_REVENUE_PER_AGENT),
      HuddleKpiHelper.LOG_TIME -> row.getAs[Long](LOG_TIME)
    )
  }

}
