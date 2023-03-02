package rever.rsparkflow.spark.domain

import rever.rsparkflow.spark.utils.TimestampUtils

case class ExecutionDateInfo(
    executionTime: Long,
    prevSuccessExecutionTime: Option[Long],
    prevExecutionTime: Option[Long],
    nextExecutionTime: Option[Long]
) {

  lazy val hourlyReportTime: Long = TimestampUtils.asStartOfHour(executionTime)
  lazy val dailyReportTime: Long = TimestampUtils.asStartOfDay(executionTime)
  lazy val weeklyReportTime: Long = TimestampUtils.asStartOfWeek(executionTime)
  lazy val monthlyReportTime: Long = TimestampUtils.asStartOfMonth(executionTime)
  lazy val quarterlyReportTime: Long = TimestampUtils.asStartOfQuarter(executionTime)
  lazy val yearlyReportTime: Long = TimestampUtils.asStartOfYear(executionTime)

  /**
    * We should sync data from the execution time to before the next execution time
    * @return
    */
  def getSyncDataTimeRange: (Long, Long) = {
    (executionTime, nextExecutionTime.getOrElse(throw new Exception("No Next Execution Time")))
  }

}
