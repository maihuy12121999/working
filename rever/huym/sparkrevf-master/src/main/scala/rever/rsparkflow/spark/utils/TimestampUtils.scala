package rever.rsparkflow.spark.utils

import rever.rsparkflow.spark.domain.IntervalTypes

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit
import java.util.{Calendar, Date, TimeZone}
import scala.collection.mutable.ListBuffer
import scala.concurrent.duration.DurationInt
import scala.util.Try

/**
  * @author anhlt (andy)
  * */
object TimestampUtils {
  private final val month_pattern = "MM/yyyy"
  private final val date_pattern = "dd/MM/yyyy"
  private final val year_pattern = "yyyy"
  private final val week_pattern = "'W'ww yyyy"

  final val MILLIS_PER_MINUTE = 1.minutes.toMillis
  final val MILLIS_PER_HOUR = 1.hours.toMillis
  final val MILLIS_PER_DAY = 1.days.toMillis

  final val DURATION_SHORT_UNITS = Seq("d", "h", "m", "s", "ms", "μs")
  final val DURATION_UNITS = Seq("days", "hours", "minutes", "seconds", "milliseconds", "microseconds")

  def getFromAndToDate(reportTime: Long, intervalType: String): (Long, Long) = {
    intervalType match {
      case IntervalTypes.DAILY     => (reportTime, reportTime)
      case IntervalTypes.WEEKLY    => (asStartOfWeek(reportTime), asEndOfWeek(reportTime))
      case IntervalTypes.MONTHLY   => (asStartOfMonth(reportTime), asEndOfMonth(reportTime))
      case IntervalTypes.QUARTERLY => (asStartOfQuarter(reportTime), asEndOfQuarter(reportTime))
      case IntervalTypes.YEARLY    => (asStartOfYear(reportTime), asEndOfYear(reportTime))
      case _                       => (reportTime, reportTime)
    }
  }

  def durationAsText(durationInMicros: Long, useShortUnit: Boolean = false): String = {

    /**
      * d
      * h
      * min
      * sec
      * millis
      * micro
      */
    val metrics = Seq(
      durationInMicros / 1.days.toMicros,
      (durationInMicros % 1.days.toMicros) / 1.hours.toMicros,
      ((durationInMicros % 1.days.toMicros) % 1.hours.toMicros) / 1.minutes.toMicros,
      (((durationInMicros % 1.days.toMicros) % 1.hours.toMicros) % 1.minutes.toMicros) / 1.seconds.toMicros,
      ((((durationInMicros % 1.days.toMicros) % 1.hours.toMicros) % 1.minutes.toMicros) % 1.seconds.toMicros) / 1.millis.toMicros,
      ((((durationInMicros % 1.days.toMicros) % 1.hours.toMicros) % 1.minutes.toMicros) % 1.seconds.toMicros) % 1.millis.toMicros
    )

    if (useShortUnit) {
      metrics
        .zip(DURATION_SHORT_UNITS)
        .filterNot(_._1 <= 0)
        .map {
          case (duration, unit) => s"$duration$unit"
        }
        .mkString
    } else {
      metrics
        .zip(DURATION_UNITS)
        .filterNot(_._1 <= 0)
        .map {
          case (duration, unit) => s"$duration $unit"
        }
        .mkString(", ")
    }
  }

  def parseMillsFromString(date: String, pattern: String): Long = {
    parseMillsFromString(date, pattern, None)
  }

  def parseMillsFromString(date: String, pattern: String, tz: Option[TimeZone]): Long = {
    val f = new SimpleDateFormat(pattern)
    tz match {
      case None    => f.setTimeZone(TimeZone.getDefault)
      case Some(x) => f.setTimeZone(x)
    }
    f.parse(date).getTime
  }

  def format(
      time: Long,
      pattern: Option[String] = Some("dd/MM/yyyy HH:mm:ss"),
      tz: Option[TimeZone] = None
  ): String = {
    val f = pattern match {
      case None    => new SimpleDateFormat("dd/MM/yyyy HH:mm:ss")
      case Some(x) => new SimpleDateFormat(x)
    }
    tz match {
      case None    => f.setTimeZone(TimeZone.getDefault)
      case Some(x) => f.setTimeZone(x)
    }
    f.format(new Date(time))
  }

  def durationToNextHour(numOfHours: Int = 1): Long = {
    val cal = Calendar.getInstance()
    cal.set(
      cal.get(Calendar.YEAR),
      cal.get(Calendar.MONTH),
      cal.get(Calendar.DAY_OF_MONTH),
      cal.get(Calendar.HOUR_OF_DAY) + numOfHours,
      0,
      0
    )
    cal.set(Calendar.MILLISECOND, 0)

    cal.getTimeInMillis - System.currentTimeMillis()
  }

  def asString(time: Long, pattern: String): String =
    TimestampUtils.format(time, Some(pattern))

  def asDate(time: Long): String = asString(time, date_pattern)

  def asWeek(time: Long): String = asString(time, week_pattern)

  def asMonth(time: Long): String = asString(time, month_pattern)

  def asYear(time: Long): String = asString(time, year_pattern)

  def asQuarter(time: Long): String = {
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(time)
    s"Q${(calendar.get(Calendar.MONTH) / 3) + 1} ${asYear(time)}"
  }

  def toStartEndOfDay(time: Long): (Long, Long) = {
    (
      TimestampUtils.asStartOfDay(time),
      TimestampUtils.asStartOfDay(time) + 1.days.toMillis - 1
    )
  }

  def asStartOfHour(time: Long): Long = {
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(time)
    calendar.set(
      calendar.get(Calendar.YEAR),
      calendar.get(Calendar.MONTH),
      calendar.get(Calendar.DAY_OF_MONTH),
      calendar.get(Calendar.HOUR_OF_DAY),
      0,
      0
    )
    calendar.set(Calendar.MILLISECOND, 0)

    calendar.getTimeInMillis
  }

  def asStartOfDay(time: Long): Long = {
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(time)
    calendar.set(
      calendar.get(Calendar.YEAR),
      calendar.get(Calendar.MONTH),
      calendar.get(Calendar.DAY_OF_MONTH),
      0,
      0,
      0
    )
    calendar.set(Calendar.MILLISECOND, 0)

    calendar.getTimeInMillis
  }

  def asEndOfMonth(time: Long): Long = {
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(time)
    calendar.set(
      calendar.get(Calendar.YEAR),
      calendar.get(Calendar.MONTH),
      calendar.getActualMaximum(Calendar.DAY_OF_MONTH),
      0,
      0,
      0
    )
    calendar.set(Calendar.MILLISECOND, 0)

    calendar.getTimeInMillis
  }

  def asStartOfMonth(time: Long): Long = {
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(time)
    calendar.set(
      calendar.get(Calendar.YEAR),
      calendar.get(Calendar.MONTH),
      1,
      0,
      0,
      0
    )
    calendar.set(Calendar.MILLISECOND, 0)

    calendar.getTimeInMillis
  }

  def toStartEndOfMonth(
      date: String,
      pattern: String = "yyyy-MM-dd"
  ): (Long, Long) = {
    toStartEndOfMonth(TimestampUtils.parseMillsFromString(date, pattern))
  }

  def toStartEndOfMonth(time: Long): (Long, Long) = {
    (
      TimestampUtils.asStartOfMonth(time),
      TimestampUtils.asEndOfMonth(time) + 1.days.toMillis - 1
    )
  }

  def toStartEndOfWeek(time: Long): (Long, Long) = {
    (
      TimestampUtils.asStartOfWeek(time),
      TimestampUtils.asEndOfWeek(time) + 1.days.toMillis - 1
    )
  }

  def asStartOfWeek(time: Long): Long = {
    val calendar = Calendar.getInstance()
    calendar.setFirstDayOfWeek(Calendar.MONDAY)
    calendar.setTimeInMillis(time)
    calendar.setFirstDayOfWeek(Calendar.MONDAY)

    calendar.set(
      calendar.get(Calendar.YEAR),
      calendar.get(Calendar.MONTH),
      calendar.get(Calendar.DAY_OF_MONTH),
      0,
      0,
      0
    )
    calendar.set(Calendar.MILLISECOND, 0)
    calendar.set(Calendar.DAY_OF_WEEK, Calendar.MONDAY)

    calendar.getTimeInMillis
  }

  def asEndOfWeek(time: Long): Long = {
    val calendar = Calendar.getInstance()
    calendar.setFirstDayOfWeek(Calendar.MONDAY)
    calendar.setTimeInMillis(time)
    calendar.setFirstDayOfWeek(Calendar.MONDAY)

    calendar.set(
      calendar.get(Calendar.YEAR),
      calendar.get(Calendar.MONTH),
      calendar.get(Calendar.DAY_OF_MONTH),
      0,
      0,
      0
    )
    calendar.set(Calendar.MILLISECOND, 0)
    calendar.set(Calendar.DAY_OF_WEEK, Calendar.SUNDAY)

    calendar.getTimeInMillis
  }

  def asStartOfQuarter(time: Long): Long = {
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(time)

    val quarter = (calendar.get(Calendar.MONTH) / 3) + 1
    val month = (quarter - 1) * 3

    calendar.set(calendar.get(Calendar.YEAR), month, 1, 0, 0, 0)
    calendar.set(Calendar.MILLISECOND, 0)

    calendar.getTimeInMillis
  }

  def asEndOfQuarter(time: Long): Long = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(time)
    cal.set(Calendar.DAY_OF_MONTH, 1)
    cal.set(Calendar.MONTH, ((cal.get(Calendar.MONTH) / 3) * 3) + 2)
    cal.set(Calendar.DAY_OF_MONTH, cal.getActualMaximum(Calendar.DAY_OF_MONTH))
    cal.set(Calendar.MILLISECOND, 0)
    cal.add(Calendar.SECOND, 0)
    cal.add(Calendar.MINUTE, 0)
    cal.add(Calendar.HOUR_OF_DAY, 0)

    cal.getTimeInMillis
  }

  def toStartEndOfQuarter(time: Long): (Long, Long) = {
    (
      TimestampUtils.asStartOfQuarter(time),
      TimestampUtils.asEndOfQuarter(time) + 1.days.toMillis - 1
    )
  }

  def asStartOfYear(time: Long): Long = {
    val calendar = Calendar.getInstance()
    calendar.setTimeInMillis(time)
    calendar.set(calendar.get(Calendar.YEAR), 0, 1, 0, 0, 0)
    calendar.set(Calendar.MILLISECOND, 0)

    calendar.getTimeInMillis
  }

  def asEndOfYear(time: Long): Long = {
    val cal = Calendar.getInstance()
    cal.setTimeInMillis(time)
    cal.set(Calendar.MONTH, 11)
    cal.set(
      cal.get(Calendar.YEAR),
      cal.get(Calendar.MONTH),
      cal.getActualMaximum(Calendar.DAY_OF_MONTH),
      0,
      0,
      0
    )
    cal.set(Calendar.MILLISECOND, 0)

    cal.getTimeInMillis
  }

  def toStartEndOfYear(time: Long): (Long, Long) = {
    (
      TimestampUtils.asStartOfYear(time),
      TimestampUtils.asEndOfYear(time) + 1.days.toMillis - 1
    )
  }

  def calcBeginOfDayInMillsFrom(mills: Long): (Long, String) = {
    val currentTime = Calendar.getInstance()
    currentTime.setTimeInMillis(mills)
    currentTime.set(Calendar.HOUR_OF_DAY, 0)
    currentTime.set(Calendar.MINUTE, 0)
    currentTime.set(Calendar.SECOND, 0)
    currentTime.set(Calendar.MILLISECOND, 0)
    val start = currentTime.getTimeInMillis

    (
      start,
      f"${currentTime.get(Calendar.DAY_OF_MONTH)}%02d-${currentTime.get(
        Calendar.MONTH
      ) + 1}%02d-${currentTime.get(Calendar.YEAR)}%04d"
    )
  }

  def makeDayRange(
      from: Long,
      to: Long,
      dayInterval: Int = 1
  ): Seq[(Long, String)] = {
    var (begin, beginStr) = calcBeginOfDayInMillsFrom(from)
    val buffer = ListBuffer.empty[(Long, String)]
    while (begin <= to) {
      if (begin >= from) {
        buffer.append((begin, beginStr))
      }
      val (time, timeStr) = calcBeginOfDayInMillsFrom(
        begin + TimeUnit.DAYS.toMillis(dayInterval)
      )
      begin = time
      beginStr = timeStr
    }
    buffer.seq
  }

  /** Split the given time range into smaller subset of time range [start, end)
    * @param from
    * @param to
    * @param stepMillis
    * @return
    */
  def chunkTimeRange(
      from: Long,
      to: Long,
      stepMillis: Long = 1
  ): Seq[(Long, Long)] = {
    val chunks = ListBuffer.empty[(Long, Long)]

    var startTime = from
    val logicalTo = to + 1
    do {

      startTime >= logicalTo match {
        case true                                          => chunks.append((startTime, logicalTo))
        case false if (startTime + stepMillis) > logicalTo => chunks.append((startTime, logicalTo))
        case _                                             => chunks.append((startTime, startTime + stepMillis))
      }
      startTime = startTime + stepMillis
    } while (startTime < logicalTo)
    chunks
  }

  def getAllQuarters(yearTime: Long): Seq[Long] = {
    val startOfYear = asStartOfYear(yearTime)
    val endOfYear = asEndOfYear(yearTime)

    val c = Calendar.getInstance()
    c.setTimeInMillis(startOfYear)

    val buffer = ListBuffer.empty[Long]

    while (c.getTimeInMillis <= endOfYear) {
      buffer.append(asStartOfQuarter(c.getTimeInMillis))

      c.add(Calendar.MONTH, 3)
    }

    buffer

  }

  def getAllMonths(yearTime: Long): Seq[Long] = {
    val startOfYear = asStartOfYear(yearTime)
    val endOfYear = asEndOfYear(yearTime)

    val c = Calendar.getInstance()
    c.setTimeInMillis(startOfYear)

    val buffer = ListBuffer.empty[Long]

    while (c.getTimeInMillis <= endOfYear) {
      buffer.append(asStartOfMonth(c.getTimeInMillis))

      c.add(Calendar.MONTH, 1)
    }

    buffer

  }

  def getAllWeeks(yearTime: Long): Seq[Long] = {
    val startOfYear = asStartOfYear(yearTime)
    val endOfYear = asEndOfYear(yearTime)

    val c = Calendar.getInstance()
    c.setTimeInMillis(startOfYear)

    val buffer = ListBuffer.empty[Long]

    while (c.getTimeInMillis <= endOfYear) {
      buffer.append(asStartOfWeek(c.getTimeInMillis))

      c.add(Calendar.WEEK_OF_YEAR, 1)
    }

    buffer

  }

  def getAllDays(yearTime: Long): Seq[Long] = {
    val startOfYear = asStartOfYear(yearTime)
    val endOfYear = asEndOfYear(yearTime)

    val c = Calendar.getInstance()
    c.setTimeInMillis(startOfYear)

    val buffer = ListBuffer.empty[Long]

    while (c.getTimeInMillis <= endOfYear) {
      buffer.append(asStartOfDay(c.getTimeInMillis))

      c.add(Calendar.DAY_OF_MONTH, 1)
    }

    buffer

  }

  def isSameYear(time: Long, time2: Long): Boolean = {
    val startOfYear1 = asStartOfYear(time)
    val startOfYear2 = asStartOfYear(time2)

    startOfYear1 == startOfYear2
  }

  def isSameQuarter(time: Long, time2: Long): Boolean = {
    val start1 = asStartOfQuarter(time)
    val start2 = asStartOfQuarter(time2)

    start1 == start2
  }

  def isSameMonth(time: Long, time2: Long): Boolean = {
    val start1 = asStartOfMonth(time)
    val start2 = asStartOfMonth(time2)

    start1 == start2
  }

  def isSameWeek(time: Long, time2: Long): Boolean = {
    val start1 = asStartOfWeek(time)
    val start2 = asStartOfWeek(time2)

    start1 == start2
  }

  def isSameDay(time: Long, time2: Long): Boolean = {
    val start1 = asStartOfDay(time)
    val start2 = asStartOfDay(time2)

    start1 == start2
  }

  def isEndOfWeek(time: Long): Boolean = {
    val startOfReportDate = asStartOfDay(time)

    startOfReportDate == asEndOfWeek(time)
  }

  def isEndOfMonth(time: Long): Boolean = {
    val startOfReportDate = asStartOfDay(time)

    startOfReportDate == asEndOfMonth(time)
  }

  def isEndOfQuarter(time: Long): Boolean = {
    val startOfReportDate = asStartOfDay(time)

    startOfReportDate == asEndOfQuarter(time)
  }

  def parseExecutionTime(executionTimeStr: String): Option[Long] = {
    val patterns = Seq(
      "yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX",
      "yyyy-MM-dd'T'HH:mm:ss.SSSXXX",
      "yyyy-MM-dd'T'HH:mm:ssXXX",
      "yyyy-MM-dd'T'HH:mm:ss",
      "yyyy-MM-dd"
    )

    patterns.foldLeft[Option[Long]](None)((r, pattern) => {
      r match {
        case Some(time) => Some(time)
        case None       => Try(parseMillsFromString(executionTimeStr, pattern)).toOption
      }
    })
  }

}
